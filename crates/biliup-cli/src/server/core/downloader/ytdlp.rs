use crate::server::errors::{AppError, AppResult};
use error_stack::{ResultExt, bail};
use std::{
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{fs, io::{AsyncBufReadExt, BufReader}, process::Command, time::timeout};
use tracing::{debug, info, warn};

#[derive(Clone, Debug, Default)]
pub struct YtDlpProgress {
    pub percent: Option<f64>,
    pub downloaded_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub speed_bps: Option<f64>,
    pub eta_sec: Option<u64>,
    pub raw_line: Option<String>,
}

pub type YtDlpProgressSink = Arc<dyn Fn(YtDlpProgress) + Send + Sync>;

#[derive(Clone, Debug)]
pub enum Backend {
    YtDlp,
    YtArchive,
}

#[derive(Clone, Debug)]
pub struct DownloadConfig {
    // URLs
    pub webpage_url: String,
    pub download_url: Option<String>,

    // 输出命名
    pub filename: String,
    pub suffix: String,
    pub working_dir: PathBuf,

    // 目录策略
    pub cache_dir: Option<PathBuf>,
    pub temp_root: PathBuf,
    pub use_temp_dir_for_ytdlp: bool,

    // 模式
    pub backend: Backend,
    pub is_live: bool,

    // 封面下载
    pub use_live_cover: bool,
    pub cover_url: Option<String>,

    // 认证与网络
    pub cookies_file: Option<PathBuf>,
    pub proxy: Option<String>,

    // yt-dlp 参数
    pub prefer_vcodec: Option<String>,
    pub prefer_acodec: Option<String>,
    pub max_filesize: Option<String>,
    pub max_height: Option<u32>,
    pub download_archive: Option<PathBuf>,
    pub two_stream_merge: bool,

    // ytarchive 参数
    pub yta_threads: u8,

    // 可执行文件名
    pub ytdlp_bin: String,
    pub ytarchive_bin: String,

    // 附加自定义参数
    pub extra_ytdlp_args: Vec<String>,
    pub extra_yta_args: Vec<String>,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            webpage_url: String::new(),
            download_url: None,
            filename: "output".into(),
            suffix: "mp4".into(),
            working_dir: PathBuf::from("."),
            cache_dir: None,
            temp_root: PathBuf::from("./cache/temp/youtube"),
            use_temp_dir_for_ytdlp: true,
            backend: Backend::YtDlp,
            is_live: false,
            use_live_cover: false,
            cover_url: None,
            cookies_file: None,
            proxy: None,
            prefer_vcodec: None,
            prefer_acodec: None,
            max_filesize: None,
            max_height: None,
            download_archive: Some(PathBuf::from("archive.txt")),
            two_stream_merge: true,
            yta_threads: 3,
            ytdlp_bin: "yt-dlp".into(),
            ytarchive_bin: "ytarchive".into(),
            extra_ytdlp_args: vec![],
            extra_yta_args: vec![],
        }
    }
}

pub struct YouTubeDownloader {
    cfg: DownloadConfig,
}

impl YouTubeDownloader {
    pub fn new(cfg: DownloadConfig) -> Self {
        Self { cfg }
    }

    pub async fn download(&self) -> AppResult<()> {
        self.download_with_progress(None).await
    }

    pub async fn download_with_progress(
        &self,
        progress_sink: Option<YtDlpProgressSink>,
    ) -> AppResult<()> {
        // 1) 可选并发封面
        let cover_handle = if self.cfg.use_live_cover {
            self.spawn_cover_download()
        } else {
            None
        };

        // 2) 执行下载
        match self.cfg.backend {
            Backend::YtArchive => self.run_ytarchive().await?,
            Backend::YtDlp => self.run_ytdlp(progress_sink).await?,
        }

        // 3) 等待封面（限时 20s）
        if let Some(handle) = cover_handle {
            match timeout(Duration::from_secs(20), handle).await {
                Ok(Ok(Ok(()))) => info!("封面已下载"),
                Ok(Ok(Err(e))) => warn!("封面下载失败: {e:#}"),
                Ok(Err(e)) => warn!("封面下载任务异常: {e:#}"),
                Err(_) => warn!("封面下载超时，继续执行"),
            }
        }

        Ok(())
    }

    async fn run_ytdlp(&self, progress_sink: Option<YtDlpProgressSink>) -> AppResult<()> {
        // 选择输出目录（临时目录 -> 搬运 -> 清理）
        let download_dir = if self.cfg.use_temp_dir_for_ytdlp {
            self.cfg.temp_root.join(&self.cfg.filename)
        } else {
            self.cfg.working_dir.clone()
        };
        fs::create_dir_all(&download_dir)
            .await
            .change_context(AppError::Custom(format!(
                "创建 yt-dlp 下载目录失败: {}",
                download_dir.display()
            )))?;

        // 构造格式串
        let format_str = if self.cfg.two_stream_merge {
            let mut s = String::from("bestvideo");
            if let Some(v) = &self.cfg.prefer_vcodec {
                s.push_str(&format!("[vcodec~='^({})']", v));
            }
            if !self.cfg.is_live
                && let Some(f) = &self.cfg.max_filesize
            {
                s.push_str(&format!("[filesize<{}]", f));
            }
            if let Some(h) = self.cfg.max_height {
                s.push_str(&format!("[height<={}]", h));
            }
            s.push_str("+bestaudio");
            if let Some(a) = &self.cfg.prefer_acodec {
                s.push_str(&format!("[acodec~='^({})']", a));
            }
            // 内置兜底：若无法拿到分离流，则回退到 b（best pre-merged）
            format!("{s}/b")
        } else {
            "best".to_string()
        };

        let use_js_runtime = self.node_available().await;
        let mut output = self
            .run_ytdlp_command(&download_dir, &format_str, use_js_runtime, progress_sink.clone())
            .await?;

        let mut combined = output.combined.clone();

        if !output.status.success()
            && use_js_runtime
            && (combined.contains("unrecognized arguments: --js-runtimes")
                || combined.contains("unknown option --js-runtimes"))
        {
            warn!("当前 yt-dlp 不支持 --js-runtimes，自动回退不启用 JS runtime");
            output = self
                .run_ytdlp_command(&download_dir, &format_str, false, progress_sink.clone())
                .await?;
            combined = output.combined.clone();
        }

        if !output.status.success() {
            if combined.contains("ffmpeg is not installed")
                || combined.contains("ffmpeg not found")
                || combined.contains("ffprobe not found")
            {
                bail!(AppError::Custom(String::from(
                    "ffmpeg 未安装或不可用，无法合并音视频"
                )));
            } else if combined.contains("Requested format is not available") {
                bail!(AppError::Custom(String::from(
                    "无法获取到流，请检查 vcodec/acodec/height/filesize 等筛选设置"
                )));
            } else {
                let mut hints = Vec::new();
                if combined.contains("nsig extraction failed")
                    || combined.contains("Precondition check failed")
                {
                    hints.push("可能是 yt-dlp 版本过旧（YouTube 频繁更新签名/接口），建议升级到最新版");
                }
                if combined.contains("HTTP Error 403")
                    || combined.contains("403: Forbidden")
                {
                    hints.push("403 可能是代理/IP 被限制、视频受限或 cookies 失效；可尝试更换代理并更新 cookies");
                }
                if combined.contains("HTTP Error 400")
                    || combined.contains("400: Bad Request")
                {
                    hints.push("400 常见于接口/签名变更或请求被拦截；优先升级 yt-dlp，并检查代理是否稳定");
                }
                if combined.contains("YouTube extraction without a JS runtime has been deprecated")
                    || combined.contains("No supported JavaScript runtime could be found")
                {
                    hints.push("当前 yt-dlp 需要 JS runtime 才能完整解析；建议安装 node/deno，并在配置里启用（本项目默认会尝试 node）");
                }

                let version = self.try_get_ytdlp_version().await;
                let mut message = format!("yt-dlp 执行失败:\n{}", combined);
                if let Some(version) = version.filter(|v| !v.trim().is_empty()) {
                    message.push_str(&format!("\n\n当前 yt-dlp 版本: {version}"));
                }
                if !hints.is_empty() {
                    message.push_str("\n\n建议：");
                    for hint in hints {
                        message.push_str("\n- ");
                        message.push_str(hint);
                    }
                }
                bail!(AppError::Custom(message));
            }
        }

        // 下载成功，必要时搬运文件到工作目录
        if self.cfg.use_temp_dir_for_ytdlp && download_dir != self.cfg.working_dir {
            self.move_dir_contents(&download_dir, &self.cfg.working_dir)
                .await
                .change_context(AppError::Custom(format!(
                    "移动下载结果失败: {} -> {}",
                    download_dir.display(),
                    self.cfg.working_dir.display()
                )))?;
        }

        // 清理临时目录
        if self.cfg.use_temp_dir_for_ytdlp
            && let Err(e) = fs::remove_dir_all(&download_dir).await
        {
            warn!(
                "清理残留文件失败，请手动删除: {}，原因: {e}",
                download_dir.display()
            );
        }

        Ok(())
    }

    async fn run_ytdlp_command(
        &self,
        download_dir: &Path,
        format_str: &str,
        use_js_runtime: bool,
        progress_sink: Option<YtDlpProgressSink>,
    ) -> AppResult<YtDlpCommandOutput> {
        let mut cmd = Command::new(&self.cfg.ytdlp_bin);
        cmd.arg("--output")
            .arg(format!(
                "{}/{}.%(ext)s",
                download_dir.display(),
                self.cfg.filename
            ))
            .arg("--newline")
            .arg("--no-color")
            .arg("--break-on-reject")
            .arg("--no-playlist")
            .args(if use_js_runtime {
                vec!["--js-runtimes", "node"]
            } else {
                Vec::new()
            })
            .arg("--format")
            .arg(format_str);

        if let Some(cookie) = &self.cfg.cookies_file {
            cmd.arg("--cookies").arg(cookie);
        }
        if let Some(proxy) = &self.cfg.proxy {
            cmd.arg("--proxy").arg(proxy);
        }
        if !self.cfg.is_live
            && let Some(archive) = &self.cfg.download_archive
        {
            cmd.arg("--download-archive").arg(archive);
        }

        for a in &self.cfg.extra_ytdlp_args {
            cmd.arg(a);
        }

        let url = self
            .cfg
            .download_url
            .as_ref()
            .unwrap_or(&self.cfg.webpage_url);
        cmd.arg(url);

        cmd.kill_on_drop(true);

        info!("运行: {:?}", cmd);
        run_streaming_command(cmd, progress_sink).await.change_context(AppError::Custom(format!(
            "运行 {} 失败，请确认已安装并在 PATH 中",
            &self.cfg.ytdlp_bin
        )))
    }

    async fn node_available(&self) -> bool {
        let output = timeout(
            Duration::from_secs(1),
            Command::new("node").arg("--version").output(),
        )
        .await;
        let Ok(Ok(output)) = output else {
            return false;
        };
        output.status.success()
    }

    async fn try_get_ytdlp_version(&self) -> Option<String> {
        let output = timeout(
            Duration::from_secs(3),
            Command::new(&self.cfg.ytdlp_bin).arg("--version").output(),
        )
        .await
        .ok()?
        .ok()?;
        if !output.status.success() {
            return None;
        }
        let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if version.is_empty() {
            None
        } else {
            Some(version)
        }
    }

    async fn run_ytarchive(&self) -> AppResult<()> {
        // ytarchive 工作目录（作为临时缓存）
        let cache_dir = self
            .cfg
            .cache_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("./cache/{}", self.cfg.filename)));
        fs::create_dir_all(&cache_dir)
            .await
            .change_context(AppError::Custom(format!(
                "创建缓存目录失败: {}",
                cache_dir.display()
            )))?;

        // 在缓存目录中执行 ytarchive
        let mut cmd = Command::new(&self.cfg.ytarchive_bin);
        cmd.current_dir(&cache_dir)
            .arg(&self.cfg.webpage_url)
            .arg("best")
            .arg("--threads")
            .arg(self.cfg.yta_threads.to_string())
            .arg("--output")
            .arg(format!("{}.{}", self.cfg.filename, self.cfg.suffix));

        if let Some(cookie) = &self.cfg.cookies_file {
            cmd.arg("--cookies").arg(cookie);
        }
        if let Some(proxy) = &self.cfg.proxy {
            cmd.arg("--proxy").arg(proxy);
        }
        cmd.arg("--add-metadata");

        // 自定义附加参数
        for a in &self.cfg.extra_yta_args {
            cmd.arg(a);
        }

        cmd.kill_on_drop(true);
        info!("运行: (cwd: {}) {:?}", cache_dir.display(), cmd);

        let output = cmd.output().await.change_context(AppError::Custom(format!(
            "运行 {} 失败，请确认已安装并在 PATH 中",
            &self.cfg.ytarchive_bin
        )))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = format!("{}\n{}", stdout, stderr);

        if !output.status.success() {
            if combined.contains("ffmpeg is not installed") || combined.contains("ffmpeg not found")
            {
                bail!(AppError::Custom(String::from(
                    "ffmpeg 未安装，ytarchive 无法合并流"
                )));
            } else {
                bail!(AppError::Custom(format!(
                    "ytarchive 执行失败:\n{}",
                    combined
                )));
            }
        }

        // 搬运到工作目录
        self.move_dir_contents(&cache_dir, &self.cfg.working_dir)
            .await
            .change_context(AppError::Custom(format!(
                "移动下载结果失败: {} -> {}",
                cache_dir.display(),
                self.cfg.working_dir.display()
            )))?;

        // 清理缓存目录
        if let Err(e) = fs::remove_dir_all(&cache_dir).await {
            warn!(
                "清理残留文件失败，请手动删除: {}，原因: {e}",
                cache_dir.display()
            );
        }

        Ok(())
    }

    async fn move_dir_contents(&self, from: &Path, to: &Path) -> AppResult<()> {
        fs::create_dir_all(to)
            .await
            .change_context(AppError::Custom(format!(
                "创建目标目录失败: {}",
                to.display()
            )))?;

        let mut entries = fs::read_dir(from).await.change_context(AppError::Unknown)?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .change_context(AppError::Unknown)?
        {
            let p = entry.path();
            let metadata = fs::metadata(&p).await.change_context(AppError::Unknown)?;
            if metadata.is_file() {
                let target = to.join(
                    p.file_name()
                        .ok_or_else(|| AppError::Custom(format!("非法文件名: {}", p.display())))?,
                );
                if let Err(_e) = fs::rename(&p, &target).await {
                    // 跨设备移动失败 -> 复制再删除
                    fs::copy(&p, &target)
                        .await
                        .change_context(AppError::Custom(format!(
                            "复制文件失败: {} -> {}",
                            p.display(),
                            target.display()
                        )))?;
                    fs::remove_file(&p)
                        .await
                        .change_context(AppError::Custom(format!(
                            "删除源文件失败: {}",
                            p.display()
                        )))?;
                    debug!("跨设备移动: {} -> {}", p.display(), target.display());
                }
            }
        }
        Ok(())
    }

    fn spawn_cover_download(&self) -> Option<tokio::task::JoinHandle<AppResult<()>>> {
        let url = self.cfg.cover_url.clone()?;

        let filename = self.cfg.filename.clone();
        let working_dir = self.cfg.working_dir.clone();

        let handle = tokio::spawn(async move {
            fs::create_dir_all(&working_dir).await.ok();

            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .change_context(AppError::Unknown)?;

            let resp = client
                .get(&url)
                .send()
                .await
                .change_context(AppError::Unknown)?;
            if !resp.status().is_success() {
                bail!(AppError::Custom(format!(
                    "封面请求失败: HTTP {}",
                    resp.status()
                )));
            }

            let content_type = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("image/jpeg")
                .to_string();

            let ext = if content_type.contains("png") {
                "png"
            } else if content_type.contains("webp") {
                "webp"
            } else {
                "jpg"
            };

            let bytes = resp.bytes().await.change_context(AppError::Unknown)?;
            let out = working_dir.join(format!("{}.{}", filename, ext));
            fs::write(&out, &bytes)
                .await
                .change_context(AppError::Custom(format!("写入封面失败: {}", out.display())))?;
            Ok(())
        });

        Some(handle)
    }
}

#[derive(Clone, Debug, Default)]
struct YtDlpCommandOutput {
    status: ExitStatus,
    combined: String,
}

async fn run_streaming_command(
    mut cmd: Command,
    progress_sink: Option<YtDlpProgressSink>,
) -> AppResult<YtDlpCommandOutput> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().change_context(AppError::Unknown)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::Custom("failed to capture stdout pipe".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| AppError::Custom("failed to capture stderr pipe".to_string()))?;

    let stdout_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut out = Vec::new();
        while let Ok(Some(line)) = lines.next_line().await {
            if out.len() < 500 {
                out.push(line);
            }
        }
        out
    });

    let stderr_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        let mut out = Vec::new();
        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.replace('\r', "");
            if let Some(sink) = progress_sink.as_ref() {
                if let Some(progress) = parse_ytdlp_progress_line(&line) {
                    sink(progress);
                }
            }
            if out.len() < 500 {
                out.push(line);
            }
        }
        out
    });

    let status = child.wait().await.change_context(AppError::Unknown)?;
    let stdout_lines = stdout_task.await.unwrap_or_default();
    let stderr_lines = stderr_task.await.unwrap_or_default();
    let combined = format!("{}\n{}", stdout_lines.join("\n"), stderr_lines.join("\n"));
    Ok(YtDlpCommandOutput { status, combined })
}

fn parse_ytdlp_progress_line(line: &str) -> Option<YtDlpProgress> {
    let trimmed = line.trim();
    if !trimmed.starts_with("[download]") {
        return None;
    }
    let rest = trimmed.trim_start_matches("[download]").trim();
    if rest.is_empty() {
        return None;
    }
    if rest.starts_with("Destination:")
        || rest.starts_with("Downloading item")
        || rest.starts_with("Resuming download")
        || rest.starts_with("Deleting existing file")
        || rest.starts_with("Total fragments")
        || rest.starts_with("Fragment")
        || rest.starts_with("Unable to resume")
        || rest.starts_with("Finished downloading")
    {
        return None;
    }

    let tokens: Vec<&str> = rest.split_whitespace().collect();
    if tokens.is_empty() {
        return None;
    }
    let pct = tokens
        .get(0)
        .and_then(|t| t.strip_suffix('%'))
        .and_then(|s| s.parse::<f64>().ok());

    let mut total_bytes: Option<u64> = None;
    let mut speed_bps: Option<f64> = None;
    let mut eta_sec: Option<u64> = None;

    if let Some(idx) = tokens.iter().position(|t| *t == "of") {
        if let Some(val) = tokens.get(idx + 1) {
            total_bytes = parse_size_bytes(val.trim_start_matches('~'));
        }
    }
    if let Some(idx) = tokens.iter().position(|t| *t == "at") {
        if let Some(val) = tokens.get(idx + 1) {
            speed_bps = parse_speed_bps(val);
        }
    }
    if let Some(idx) = tokens.iter().position(|t| *t == "ETA") {
        if let Some(val) = tokens.get(idx + 1) {
            eta_sec = parse_eta_seconds(val);
        }
    }

    let downloaded_bytes = match (pct, total_bytes) {
        (Some(p), Some(t)) => {
            let frac = (p / 100.0).clamp(0.0, 1.0);
            Some((t as f64 * frac).round() as u64)
        }
        _ => None,
    };

    Some(YtDlpProgress {
        percent: pct,
        downloaded_bytes,
        total_bytes,
        speed_bps,
        eta_sec,
        raw_line: Some(rest.to_string()),
    })
}

fn parse_size_bytes(token: &str) -> Option<u64> {
    let t = token.trim().trim_end_matches("/s");
    if t.is_empty() {
        return None;
    }
    let pos = t
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(t.len());
    let (num, unit) = t.split_at(pos);
    let value = num.parse::<f64>().ok()?;
    let unit = unit.trim();
    let mult: f64 = match unit {
        "B" => 1.0,
        "KiB" | "KB" => 1024.0,
        "MiB" | "MB" => 1024.0 * 1024.0,
        "GiB" | "GB" => 1024.0 * 1024.0 * 1024.0,
        "TiB" | "TB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    Some((value * mult).round().max(0.0) as u64)
}

fn parse_speed_bps(token: &str) -> Option<f64> {
    let t = token.trim();
    let t = t.trim_end_matches("/s");
    parse_size_bytes(t).map(|b| b as f64)
}

fn parse_eta_seconds(token: &str) -> Option<u64> {
    let t = token.trim();
    if t.is_empty() {
        return None;
    }
    let parts: Vec<&str> = t.split(':').collect();
    match parts.len() {
        2 => {
            let m = parts[0].parse::<u64>().ok()?;
            let s = parts[1].parse::<u64>().ok()?;
            Some(m.saturating_mul(60).saturating_add(s))
        }
        3 => {
            let h = parts[0].parse::<u64>().ok()?;
            let m = parts[1].parse::<u64>().ok()?;
            let s = parts[2].parse::<u64>().ok()?;
            Some(h.saturating_mul(3600).saturating_add(m.saturating_mul(60)).saturating_add(s))
        }
        _ => None,
    }
}
