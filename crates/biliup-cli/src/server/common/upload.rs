use crate::UploadLine;
use crate::server::common::util::Recorder;
use crate::server::config::Config;
use crate::server::core::downloader::SegmentInfo;
use crate::server::errors::{AppError, AppResult};
use crate::server::infrastructure::context::{Context, Stage, Worker, WorkerStatus};
use crate::server::infrastructure::models::InsertFileItem;
use crate::server::infrastructure::models::hook_step::process_video;
use crate::server::infrastructure::models::upload_streamer::UploadStreamer;
use async_channel::Receiver;
use biliup::bilibili::{BiliBili, ResponseData, Studio, Video};
use biliup::client::StatelessClient;
use biliup::credential::login_by_cookies;
use biliup::error::Kind;
use biliup::uploader::line::{Line, Probe};
use biliup::uploader::util::SubmitOption;
use biliup::uploader::{VideoFile, line};
use error_stack::ResultExt;
use futures::StreamExt;
use futures::stream::Inspect;
use ormlite::Insert;
use serde::Deserialize;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;
use tokio::pin;
use tracing::{error, info, warn};

const YOUTUBE_TITLE_STRATEGY_DEEPSEEK: &str = "deepseek_translate_polish";
const DEEPSEEK_CHAT_URL: &str = "https://api.deepseek.com/chat/completions";
const DEEPSEEK_SYSTEM_PROMPT: &str =
    "你是一个中文视频标题编辑，请把输入标题翻译成简体中文并润色成适合B站投稿的标题。\
只输出一行最终标题，不要解释，不要加引号，不要加多余前后缀。";
const DEEPSEEK_DEFAULT_USER_PROMPT: &str = "请将以下 YouTube 标题翻译成简体中文并润色，\
保留核心信息和专有名词，控制在 80 字以内：\n{title}";

#[derive(Debug, Deserialize)]
struct DeepSeekChatResponse {
    choices: Vec<DeepSeekChoice>,
}

#[derive(Debug, Deserialize)]
struct DeepSeekChoice {
    message: DeepSeekMessage,
}

#[derive(Debug, Deserialize)]
struct DeepSeekMessage {
    content: Option<String>,
}

// 辅助结构体
struct UploadContext {
    bilibili: BiliBili,
    line: Line,
    threads: usize,
    client: StatelessClient,
}

#[derive(Default)]
struct UploadedVideos {
    videos: Vec<Video>,
    paths: Vec<PathBuf>,
}

pub async fn process_with_upload<F>(
    rx: Inspect<Receiver<SegmentInfo>, F>,
    ctx: &Context,
    upload_config: &UploadStreamer,
) -> AppResult<()>
where
    F: FnMut(&SegmentInfo),
{
    info!(upload_config=?upload_config, "Starting process with upload");
    // 1. 初始化上传环境
    let upload_context =
        initialize_upload_context(&ctx.config(), &ctx.stateless_client(), upload_config).await?;

    // 2. 流水线处理视频上传
    let uploaded_videos = pipeline_upload_videos(rx, &upload_context).await?;

    // 3. 提交到B站
    if !uploaded_videos.videos.is_empty() {
        let mut recorder = ctx
            .recorder(ctx.stream_info_ext().streamer_info.clone())
            .clone();
        recorder.filename_prefix = upload_config.title.clone();
        let current_config = ctx.config();

        let studio = build_studio(
            &current_config,
            &upload_config,
            &upload_context.bilibili,
            uploaded_videos.videos,
            &recorder,
        )
        .await?;
        let submit_api = ctx.config().submit_api.clone();
        submit_to_bilibili(&upload_context.bilibili, &studio, submit_api.as_deref()).await?;
    }

    // 4. 执行后处理
    if !uploaded_videos.paths.is_empty() {
        execute_postprocessor(uploaded_videos.paths, ctx).await?;
    }

    Ok(())
}

async fn initialize_upload_context(
    config: &Config,
    client: &StatelessClient,
    upload_config: &UploadStreamer,
) -> AppResult<UploadContext> {
    // 登录处理
    let cookie_file = upload_config
        .user_cookie
        .clone()
        .unwrap_or("cookies.json".to_string());
    let bilibili = login_by_cookies(&cookie_file, None)
        .await
        .change_context(AppError::Unknown)?;

    // 获取上传线路
    let line = get_upload_line(&client.client, &config.lines).await?;

    Ok(UploadContext {
        bilibili,
        line,
        threads: config.threads as usize,
        client: client.clone(),
    })
}

async fn get_upload_line(client: &reqwest::Client, line: &str) -> AppResult<Line> {
    let line = match line {
        "bda2" => line::bda2(),
        "bda" => line::bda(),
        "tx" => line::tx(),
        "txa" => line::txa(),
        "bldsa" => line::bldsa(),
        "alia" => line::alia(),
        _ => Probe::probe(client).await.unwrap_or_default(),
    };
    Ok(line)
}

async fn pipeline_upload_videos<F>(
    rx: Inspect<Receiver<SegmentInfo>, F>,
    context: &UploadContext,
) -> AppResult<UploadedVideos>
where
    F: FnMut(&SegmentInfo),
{
    // let mut desc_v2 = Vec::new();
    // for credit in context.upload_config.desc_v2_credit {
    //     desc_v2.push(Credit {
    //         type_id: credit.type_id,
    //         raw_text: credit.raw_text,
    //         biz_id: credit.biz_id,
    //     });
    // }

    let mut uploaded = UploadedVideos::default();
    pin!(rx);
    // 流式处理后续事件
    while let Some(event) = rx.next().await {
        let video = upload_single_file(&event.prev_file_path, context).await?;
        uploaded.videos.push(video);
        uploaded.paths.push(event.prev_file_path);
        // 失败的文件不加入路径列表，避免后处理出错
    }

    Ok(uploaded)
}

async fn upload_single_file(file_path: &Path, context: &UploadContext) -> AppResult<Video> {
    let video_path = file_path;
    let UploadContext {
        bilibili,
        line,
        threads: limit,
        client,
    } = context;

    info!(
        "开始上传文件：{:?}",
        video_path
            .canonicalize()
            .change_context(AppError::Unknown)?
            .to_str()
    );
    info!("线路选择：{line:?}");
    let video_file = VideoFile::new(video_path).change_context(AppError::Unknown)?;
    let total_size = video_file.total_size;
    let file_name = video_file.file_name.clone();
    let uploader = line
        .pre_upload(bilibili, video_file)
        .await
        .change_context(AppError::Unknown)?;

    let instant = Instant::now();

    let video = uploader
        .upload(client.clone(), *limit, |vs| {
            vs.map(|vs| {
                let chunk = vs?;
                let len = chunk.len();
                Ok((chunk, len))
            })
        })
        .await
        .change_context(AppError::Unknown)?;
    let t = instant.elapsed().as_millis();
    info!(
        "Upload completed: {file_name} => cost {:.2}s, {:.2} MB/s.",
        t as f64 / 1000.,
        total_size as f64 / 1000. / t as f64
    );
    Ok(video)
}

pub async fn submit_to_bilibili(
    bilibili: &BiliBili,
    studio: &Studio,
    submit_api: Option<&str>,
) -> AppResult<ResponseData> {
    // let submit = match worker.config.read().unwrap().submit_api {
    //     Some(submit) => SubmitOption::from_str(&submit).unwrap_or(SubmitOption::App),
    //     _ => SubmitOption::App,
    // };

    // let submit_result = match submit {
    //     SubmitOption::BCutAndroid => {
    //         bilibili.submit_by_bcut_android(&studio, None).await
    //     }
    //     _ => bilibili.submit_by_app(&studio, None).await,
    // };

    let submit_option = match submit_api {
        Some(submit) => SubmitOption::from_str(submit).unwrap_or(SubmitOption::App),
        _ => SubmitOption::App,
    };

    let result = match submit_option {
        SubmitOption::BCutAndroid => bilibili
            .submit_by_bcut_android(studio, None)
            .await
            .change_context(AppError::Unknown)?,
        _ => bilibili
            .submit_by_app(studio, None)
            .await
            .change_context(AppError::Unknown)?,
    };
    info!("Submit successful");
    Ok(result)
}

pub(crate) async fn build_studio(
    config: &Config,
    upload_config: &UploadStreamer,
    bilibili: &BiliBili,
    videos: Vec<Video>,
    recorder: &Recorder,
) -> AppResult<Studio> {
    let optimized_title = maybe_optimize_youtube_title(config, upload_config, recorder).await;
    // 使用 Builder 模式简化构建
    let mut studio: Studio = Studio::builder()
        .desc(recorder.format(&upload_config.description.clone().unwrap_or_default()))
        .maybe_dtime(upload_config.dtime)
        .maybe_copyright(upload_config.copyright)
        .cover(upload_config.cover_path.clone().unwrap_or_default())
        .dynamic(upload_config.dynamic.clone().unwrap_or_default())
        .source(
            upload_config
                .copyright_source
                .clone()
                .unwrap_or_else(|| recorder.streamer_info.url.clone()),
        )
        .tag(upload_config.tags.join(","))
        .maybe_tid(upload_config.tid)
        .title(optimized_title)
        .videos(videos)
        .dolby(upload_config.dolby.unwrap_or_default())
        // .lossless_music(upload_config.)
        .no_reprint(upload_config.no_reprint.unwrap_or_default())
        .charging_pay(upload_config.charging_pay.unwrap_or_default())
        .up_close_reply(upload_config.up_close_reply.unwrap_or_default())
        .up_selection_reply(upload_config.up_selection_reply.unwrap_or_default())
        .up_close_danmu(upload_config.up_close_danmu.unwrap_or_default())
        .maybe_is_only_self(upload_config.is_only_self)
        .maybe_desc_v2(None)
        .extra_fields(
            serde_json::from_str(&upload_config.extra_fields.clone().unwrap_or_default())
                .unwrap_or_default(), // 处理额外字段
        )
        .build();
    // 处理封面上传
    if !studio.cover.is_empty()
        && let Ok(c) = &std::fs::read(&studio.cover).inspect_err(|e| error!(e=?e))
        && let Ok(url) = bilibili.cover_up(c).await.inspect_err(|e| error!(e=?e))
    {
        studio.cover = url;
    };

    Ok(studio)
}

async fn maybe_optimize_youtube_title(
    config: &Config,
    upload_config: &UploadStreamer,
    recorder: &Recorder,
) -> String {
    let default_title = recorder.format_filename();
    let Some(strategy) = upload_config.youtube_title_strategy.as_deref() else {
        return default_title;
    };
    if strategy != YOUTUBE_TITLE_STRATEGY_DEEPSEEK {
        return default_title;
    }
    if !is_youtube_url(&recorder.streamer_info.url) {
        return default_title;
    }

    let source_title = recorder.streamer_info.title.trim();
    if source_title.is_empty() {
        warn!("youtube title strategy enabled, but source title is empty");
        return default_title;
    }

    let api_key = config
        .deepseek_api_key
        .clone()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| std::env::var("DEEPSEEK_API_KEY").ok().filter(|v| !v.trim().is_empty()));
    let Some(api_key) = api_key else {
        warn!("youtube title strategy enabled, but deepseek api key is not configured");
        return default_title;
    };

    let api_base = config
        .deepseek_api_base
        .as_deref()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or(DEEPSEEK_CHAT_URL);
    let model = config
        .deepseek_model
        .as_deref()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or("deepseek-chat");

    let user_prompt = build_user_prompt(
        upload_config.youtube_title_strategy_prompt.as_deref(),
        source_title,
    );

    let client = reqwest::Client::new();
    let payload = json!({
        "model": model,
        "messages": [
            {"role": "system", "content": DEEPSEEK_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.7
    });

    let response = match client
        .post(api_base)
        .bearer_auth(api_key)
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            warn!(error = ?e, "deepseek request failed");
            return default_title;
        }
    };

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        warn!(%status, %body, "deepseek response is not successful");
        return default_title;
    }

    let parsed = match response.json::<DeepSeekChatResponse>().await {
        Ok(data) => data,
        Err(e) => {
            warn!(error = ?e, "failed to parse deepseek response");
            return default_title;
        }
    };

    let Some(content) = parsed
        .choices
        .first()
        .and_then(|c| c.message.content.as_deref())
    else {
        warn!("deepseek returned empty choices");
        return default_title;
    };

    let polished_title = normalize_single_line(content);
    if polished_title.is_empty() {
        warn!("deepseek returned empty content");
        return default_title;
    }

    // 优先让模板中的 {title} 使用 AI 结果；如果模板未使用 {title}，直接用 AI 结果作为稿件标题。
    let template = upload_config.title.clone().unwrap_or_default();
    if template.contains("{title}") {
        let mut recorder_for_ai_title = recorder.clone();
        recorder_for_ai_title.streamer_info.title = truncate_chars(&polished_title, 80);
        return truncate_chars(&recorder_for_ai_title.format_filename(), 80);
    }
    truncate_chars(&polished_title, 80)
}

fn is_youtube_url(url: &str) -> bool {
    let u = url.to_ascii_lowercase();
    u.contains("youtube.com") || u.contains("youtu.be")
}

fn build_user_prompt(custom_prompt: Option<&str>, title: &str) -> String {
    let prompt = custom_prompt
        .filter(|p| !p.trim().is_empty())
        .unwrap_or(DEEPSEEK_DEFAULT_USER_PROMPT);
    if prompt.contains("{title}") {
        return prompt.replace("{title}", title);
    }
    format!("{prompt}\n\n原始标题：{title}")
}

fn normalize_single_line(text: &str) -> String {
    text.replace('\r', " ")
        .replace('\n', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

fn truncate_chars(s: &str, max_len: usize) -> String {
    s.chars().take(max_len).collect()
}

pub async fn execute_postprocessor(video_paths: Vec<PathBuf>, ctx: &Context) -> AppResult<()> {
    if let Some(processor) = &ctx.live_streamer().postprocessor {
        let paths: Vec<&Path> = video_paths.iter().map(|p| p.as_path()).collect();
        process_video(&paths, processor).await?;
    }
    Ok(())
}

pub async fn upload(
    cookie_file: impl AsRef<Path>,
    proxy: Option<&str>,
    line: Option<UploadLine>,
    video_paths: &[PathBuf],
    limit: usize,
) -> AppResult<(BiliBili, Vec<Video>)> {
    let bilibili = login_by_cookies(&cookie_file, proxy).await;
    let bilibili = match bilibili {
        Err(Kind::IO(_)) => bilibili.change_context_lazy(|| {
            AppError::Custom(format!(
                "open cookies file: {}",
                &cookie_file.as_ref().to_string_lossy()
            ))
        })?,
        _ => bilibili.change_context_lazy(|| AppError::Unknown)?,
    };

    let client = StatelessClient::default();
    let mut videos = Vec::new();
    let line = match line {
        Some(UploadLine::Bldsa) => line::bldsa(),
        Some(UploadLine::Cnbldsa) => line::cnbldsa(),
        Some(UploadLine::Andsa) => line::andsa(),
        Some(UploadLine::Atdsa) => line::atdsa(),
        Some(UploadLine::Bda2) => line::bda2(),
        Some(UploadLine::Cnbd) => line::cnbd(),
        Some(UploadLine::Anbd) => line::anbd(),
        Some(UploadLine::Atbd) => line::atbd(),
        Some(UploadLine::Tx) => line::tx(),
        Some(UploadLine::Cntx) => line::cntx(),
        Some(UploadLine::Antx) => line::antx(),
        Some(UploadLine::Attx) => line::attx(),
        // Some(UploadLine::Bda) => line::bda(),
        Some(UploadLine::Txa) => line::txa(),
        Some(UploadLine::Alia) => line::alia(),
        _ => Probe::probe(&client.client).await.unwrap_or_default(),
    };
    for video_path in video_paths {
        println!(
            "{:?}",
            video_path
                .canonicalize()
                .change_context_lazy(|| AppError::Unknown)?
                .to_str()
        );
        info!("{line:?}");
        let video_file = VideoFile::new(video_path).change_context_lazy(|| AppError::Unknown)?;
        let total_size = video_file.total_size;
        let file_name = video_file.file_name.clone();
        let uploader = line
            .pre_upload(&bilibili, video_file)
            .await
            .change_context_lazy(|| AppError::Unknown)?;

        let instant = Instant::now();

        let video = uploader
            .upload(client.clone(), limit, |vs| {
                vs.map(|vs| {
                    let chunk = vs?;
                    let len = chunk.len();
                    Ok((chunk, len))
                })
            })
            .await
            .change_context_lazy(|| AppError::Unknown)?;
        let t = instant.elapsed().as_millis();
        info!(
            "Upload completed: {file_name} => cost {:.2}s, {:.2} MB/s.",
            t as f64 / 1000.,
            total_size as f64 / 1000. / t as f64
        );
        videos.push(video);
    }

    Ok((bilibili, videos))
}

/// 上传Actor
/// 负责处理上传相关的消息和任务
pub struct UActor {
    /// 上传消息接收器
    receiver: Receiver<UploaderMessage>,
}

impl UActor {
    /// 创建新的上传Actor实例
    pub fn new(receiver: Receiver<UploaderMessage>) -> Self {
        Self { receiver }
    }

    /// 运行Actor主循环，处理接收到的消息
    pub(crate) async fn run(&mut self) {
        while let Ok(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    /// 处理上传消息
    ///
    /// # 参数
    /// * `msg` - 要处理的上传消息
    async fn handle_message(&mut self, msg: UploaderMessage) {
        match msg {
            UploaderMessage::SegmentEvent(rx, ctx) => {
                ctx.change_status(Stage::Upload, WorkerStatus::Pending)
                    .await;
                let inspect = rx.inspect(|f| {
                    let pool = ctx.pool().clone();
                    let streamer_info_id = ctx.id();
                    let file = f.prev_file_path.display().to_string();
                    tokio::spawn(async move {
                        let result = InsertFileItem {
                            file,
                            streamer_info_id,
                        }
                        .insert(&pool)
                        .await;
                        info!(result=?result, "Insert file");
                    });
                });
                let result = match ctx.upload_config() {
                    Some(config) => process_with_upload(inspect, &ctx, config).await,
                    None => {
                        let mut paths = Vec::new();
                        pin!(inspect);
                        while let Some(event) = inspect.next().await {
                            paths.push(event.prev_file_path);
                        }
                        // 无上传配置时，直接执行后处理
                        execute_postprocessor(paths, &ctx).await
                    }
                };

                if let Err(e) = &result {
                    error!("Process segment event failed: {}", e);
                    // 可以添加错误通知机制
                }
                info!(url=ctx.live_streamer().url, result=?result, "后处理执行完毕：Finished processing segment event");
                ctx.change_status(Stage::Upload, WorkerStatus::Idle).await;
            }
        }
    }
}

/// 上传消息枚举
/// 定义上传Actor可以处理的消息类型
#[derive(Debug)]
pub enum UploaderMessage {
    /// 分段事件消息，包含事件、接收器和工作器
    SegmentEvent(Receiver<SegmentInfo>, Context),
}
