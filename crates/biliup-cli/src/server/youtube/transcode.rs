use crate::server::core::downloader::ffmpeg_downloader::{FfmpegProgress, ProgressSink};
use crate::server::errors::{AppError, AppResult};
use error_stack::ResultExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

const EFFECT_PRESET: &str = "veryfast";
const EFFECT_WINDOW_COUNT_MIN: usize = 1;
const EFFECT_WINDOW_COUNT_MAX: usize = 2;
const EFFECT_WINDOW_SPAN_MIN_SEC: f64 = 0.12;
const EFFECT_WINDOW_SPAN_MAX_SEC: f64 = 0.25;
const TIME_EPSILON_SEC: f64 = 0.0005;

#[derive(Debug, Clone)]
pub struct ProbeSummary {
    pub format_name: String,
    pub duration_sec: f64,
    pub size_bytes: u64,
    pub overall_bps: Option<u64>,
    pub video_codec: Option<String>,
    pub video_pix_fmt: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub video_bps: Option<u64>,
    pub audio_codec: Option<String>,
    pub audio_bps: Option<u64>,
}

impl ProbeSummary {
    pub fn pretty(&self) -> String {
        let format = if self.format_name.is_empty() {
            "-".to_string()
        } else {
            self.format_name.clone()
        };
        let video = match (&self.video_codec, &self.video_pix_fmt, self.width, self.height) {
            (Some(codec), Some(pix), Some(w), Some(h)) => format!("{codec}/{pix} {w}x{h}"),
            (Some(codec), _, Some(w), Some(h)) => format!("{codec} {w}x{h}"),
            (Some(codec), _, _, _) => codec.clone(),
            _ => "-".to_string(),
        };
        let audio = match (&self.audio_codec, self.audio_bps) {
            (Some(codec), Some(bps)) => format!("{codec} {}kbps", (bps / 1000).max(1)),
            (Some(codec), None) => codec.clone(),
            _ => "无音频".to_string(),
        };
        let size_mb = self.size_bytes as f64 / 1024.0 / 1024.0;
        let dur = self.duration_sec.max(0.0);
        let br = self.overall_bps.unwrap_or(0) as f64 / 1_000_000.0;
        if br > 0.0 {
            format!("{format}; {video}; {audio}; {:.1}MB; {:.1}s; {:.2}Mbps", size_mb, dur, br)
        } else {
            format!("{format}; {video}; {audio}; {:.1}MB; {:.1}s", size_mb, dur)
        }
    }
}

#[derive(Debug, Clone)]
pub enum AudioPlan {
    Copy,
    Aac { bps: u64 },
    None,
}

#[derive(Debug, Clone)]
pub struct FfmpegReport {
    pub output: PathBuf,
    pub input: ProbeSummary,
    pub output_summary: ProbeSummary,
    pub elapsed_ms: u128,
    pub args: Vec<String>,
    pub target_video_bps: u64,
    pub audio_plan: AudioPlan,
    pub filter: Option<String>,
}

#[derive(Debug, Clone)]
struct EffectWindow {
    start_sec: f64,
    end_sec: f64,
    x: u32,
    y: u32,
    width: u32,
    height: u32,
    alpha: f64,
}

#[derive(Debug, Clone)]
enum SegmentPlanKind {
    Copy,
    Reencode { windows: Vec<EffectWindow> },
}

#[derive(Debug, Clone)]
struct SegmentPlan {
    start_sec: f64,
    end_sec: f64,
    kind: SegmentPlanKind,
}

pub async fn probe_summary(path: &Path) -> AppResult<ProbeSummary> {
    let value = probe_value(path).await?;
    Ok(summary_from_probe(&value).with_size_from_fs(path).await)
}

pub fn need_transcode(summary: &ProbeSummary) -> bool {
    let supported_container = summary
        .format_name
        .split(',')
        .any(|name| matches!(name.trim(), "mp4" | "mov" | "matroska"));
    let has_h264_video = summary.video_codec.as_deref() == Some("h264");
    let has_yuv420p = summary.video_pix_fmt.as_deref() == Some("yuv420p");
    let has_aac_audio = summary.audio_codec.is_none() || summary.audio_codec.as_deref() == Some("aac");
    !(supported_container && has_h264_video && has_yuv420p && has_aac_audio)
}

pub async fn transcode_with_report(
    video_id: &str,
    input: &Path,
    progress_sink: Option<ProgressSink>,
) -> AppResult<FfmpegReport> {
    let input_summary = probe_summary(input).await?;
    let output = input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{video_id}.upload.mp4"));

    let audio_plan = decide_audio_plan(&input_summary, false);
    let (target_video_bps, target_audio_bps) = decide_target_bitrates(&input_summary, &audio_plan, 1.10);

    let preset = "slow";
    let mut args = vec![
        "-y".to_string(),
        "-progress".to_string(),
        "pipe:2".to_string(),
        "-nostats".to_string(),
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-i".to_string(),
        input.to_string_lossy().to_string(),
        "-map".to_string(),
        "0:v:0".to_string(),
        "-map".to_string(),
        "0:a:0?".to_string(),
        "-c:v".to_string(),
        "libx264".to_string(),
        "-preset".to_string(),
        preset.to_string(),
        "-b:v".to_string(),
        format!("{target_video_bps}"),
        "-maxrate".to_string(),
        format!("{}", (target_video_bps as f64 * 1.15).round() as u64),
        "-bufsize".to_string(),
        format!("{}", target_video_bps.saturating_mul(2)),
        "-pix_fmt".to_string(),
        "yuv420p".to_string(),
    ];
    push_audio_args(&mut args, &audio_plan, target_audio_bps);
    args.extend([
        "-movflags".to_string(),
        "+faststart".to_string(),
        output.to_string_lossy().to_string(),
    ]);

    let start = Instant::now();
    run_ffmpeg_with_progress(&args, progress_sink)
        .await
        .change_context(AppError::Custom(
            "执行 ffmpeg 转码失败，请确认已安装 ffmpeg".to_string(),
        ))?;

    let output_summary = probe_summary(&output).await?;
    Ok(FfmpegReport {
        output,
        input: input_summary,
        output_summary,
        elapsed_ms: start.elapsed().as_millis(),
        args,
        target_video_bps,
        audio_plan,
        filter: None,
    })
}

pub async fn apply_upload_effects_with_report(
    video_id: &str,
    input: &Path,
    progress_sink: Option<ProgressSink>,
) -> AppResult<FfmpegReport> {
    let input_summary = probe_summary(input).await?;
    let (width, height, duration_sec) = (
        input_summary.width.unwrap_or(1920),
        input_summary.height.unwrap_or(1080),
        input_summary.duration_sec.max(5.0),
    );
    let effect_windows = build_effect_windows(video_id, width, height, duration_sec);
    let filter = build_effect_filter(&effect_windows, 0.0, duration_sec);
    let output = input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{video_id}.fx.mp4"));

    let audio_plan = decide_audio_plan(&input_summary, true);
    let (target_video_bps, target_audio_bps) = decide_target_bitrates(&input_summary, &audio_plan, 1.02);
    let keyframes = probe_video_keyframes(input).await?;
    let plan = build_segment_plan(duration_sec, &keyframes, &effect_windows);

    let start = Instant::now();
    let work_dir = input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(".{video_id}.fx.parts"));
    if let Err(err) = fs::remove_dir_all(&work_dir).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        return Err(AppError::Custom(format!(
            "清理旧的视频处理临时目录失败: {}",
            err
        ))
        .into());
    }
    fs::create_dir_all(&work_dir)
        .await
        .change_context(AppError::Custom("创建视频处理临时目录失败".to_string()))?;

    let total_duration_ms = (duration_sec * 1000.0).round().max(1.0) as u64;
    let run_result = run_effect_segment_pipeline(
        input,
        &output,
        &work_dir,
        &plan,
        &input_summary,
        &audio_plan,
        target_video_bps,
        target_audio_bps,
        progress_sink,
        total_duration_ms,
    )
    .await;
    if let Err(err) = fs::remove_dir_all(&work_dir).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        eprintln!("cleanup effect work dir failed: {}", err);
    }
    run_result?;

    let output_summary = probe_summary(&output).await?;
    Ok(FfmpegReport {
        output,
        input: input_summary,
        output_summary,
        elapsed_ms: start.elapsed().as_millis(),
        args: vec![
            "partial-blackout".to_string(),
            format!("segments={}", plan.len()),
            format!("drawbox={}", effect_windows.len()),
            format!("preset={EFFECT_PRESET}"),
        ],
        target_video_bps,
        audio_plan,
        filter: Some(filter),
    })
}

async fn run_effect_segment_pipeline(
    input: &Path,
    output: &Path,
    work_dir: &Path,
    plan: &[SegmentPlan],
    input_summary: &ProbeSummary,
    audio_plan: &AudioPlan,
    target_video_bps: u64,
    target_audio_bps: Option<u64>,
    progress_sink: Option<ProgressSink>,
    total_duration_ms: u64,
) -> AppResult<()> {
    let mut segment_paths = Vec::with_capacity(plan.len());
    for (index, segment) in plan.iter().enumerate() {
        let segment_path = work_dir.join(format!("seg_{index:03}.ts"));
        let segment_end_ms = (segment.end_sec * 1000.0).round().max(0.0) as u64;
        match &segment.kind {
            SegmentPlanKind::Copy => {
                export_copy_segment_to_ts(input, segment.start_sec, segment.end_sec, &segment_path)
                    .await?;
                emit_progress_checkpoint(progress_sink.as_ref(), segment_end_ms);
            }
            SegmentPlanKind::Reencode { windows } => {
                export_effect_segment_to_ts(
                    input,
                    segment,
                    windows,
                    &segment_path,
                    audio_plan,
                    target_video_bps,
                    target_audio_bps,
                    progress_sink.as_ref(),
                    total_duration_ms,
                )
                .await?;
                emit_progress_checkpoint(progress_sink.as_ref(), segment_end_ms);
            }
        }
        segment_paths.push(segment_path);
    }

    concat_segments_to_mp4(
        &segment_paths,
        output,
        input_summary.audio_codec.is_some(),
        progress_sink.as_ref(),
        total_duration_ms,
    )
    .await
}

async fn probe_value(path: &Path) -> AppResult<Value> {
    let output = Command::new("ffprobe")
        .kill_on_drop(true)
        .arg("-v")
        .arg("quiet")
        .arg("-print_format")
        .arg("json")
        .arg("-show_streams")
        .arg("-show_format")
        .arg(path)
        .output()
        .await
        .change_context(AppError::Custom(
            "执行 ffprobe 失败，请确认已安装 ffmpeg/ffprobe".to_string(),
        ))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(AppError::Custom(format!("ffprobe 失败: {stderr}")).into());
    }

    serde_json::from_slice::<Value>(&output.stdout)
        .change_context(AppError::Custom("ffprobe 输出解析失败".to_string()))
}

async fn probe_video_keyframes(path: &Path) -> AppResult<Vec<f64>> {
    let output = Command::new("ffprobe")
        .kill_on_drop(true)
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("v:0")
        .arg("-show_packets")
        .arg("-show_entries")
        .arg("packet=pts_time,flags")
        .arg("-of")
        .arg("csv=p=0")
        .arg(path)
        .output()
        .await
        .change_context(AppError::Custom(
            "执行 ffprobe 关键帧探测失败，请确认已安装 ffmpeg/ffprobe".to_string(),
        ))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(AppError::Custom(format!("ffprobe 关键帧探测失败: {stderr}")).into());
    }

    let mut points = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let mut fields = line.split(',');
        let Some(ts_raw) = fields.next() else {
            continue;
        };
        let Some(flags) = fields.next() else {
            continue;
        };
        if !flags.contains('K') {
            continue;
        }
        if let Ok(ts) = ts_raw.trim().parse::<f64>()
            && ts.is_finite()
            && ts >= 0.0
        {
            points.push(ts);
        }
    }
    points.sort_by(|a, b| a.total_cmp(b));
    points.dedup_by(|a, b| (*a - *b).abs() <= TIME_EPSILON_SEC);
    Ok(points)
}

fn summary_from_probe(value: &Value) -> ProbeSummary {
    let streams = value
        .get("streams")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let video = streams
        .iter()
        .find(|stream| stream.get("codec_type").and_then(|v| v.as_str()) == Some("video"));
    let audio = streams
        .iter()
        .find(|stream| stream.get("codec_type").and_then(|v| v.as_str()) == Some("audio"));

    let width = video
        .and_then(|v| v.get("width"))
        .and_then(|v| v.as_u64())
        .map(|v| v as u32);
    let height = video
        .and_then(|v| v.get("height"))
        .and_then(|v| v.as_u64())
        .map(|v| v as u32);
    let duration_sec = value
        .get("format")
        .and_then(|v| v.get("duration"))
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0);

    let format_name = value
        .get("format")
        .and_then(|v| v.get("format_name"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let size_bytes = value
        .get("format")
        .and_then(|v| v.get("size"))
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let overall_bps = value
        .get("format")
        .and_then(|v| v.get("bit_rate"))
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<u64>().ok());

    let video_codec = video
        .and_then(|v| v.get("codec_name"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let video_pix_fmt = video
        .and_then(|v| v.get("pix_fmt"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let video_bps = video
        .and_then(|v| v.get("bit_rate"))
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<u64>().ok());

    let audio_codec = audio
        .and_then(|v| v.get("codec_name"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let audio_bps = audio
        .and_then(|v| v.get("bit_rate"))
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<u64>().ok());

    ProbeSummary {
        format_name,
        duration_sec,
        size_bytes,
        overall_bps,
        video_codec,
        video_pix_fmt,
        width,
        height,
        video_bps,
        audio_codec,
        audio_bps,
    }
}

async fn run_ffmpeg_with_progress(
    args: &[String],
    progress_sink: Option<ProgressSink>,
) -> AppResult<()> {
    let mut cmd = Command::new("ffmpeg");
    cmd.kill_on_drop(true)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().change_context(AppError::Unknown)?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| AppError::Custom("failed to capture ffmpeg stderr".to_string()))?;

    let mut lines = BufReader::new(stderr).lines();
    let mut progress = FfmpegProgress::default();
    let mut tail: Vec<String> = Vec::new();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some((k, v)) = line.split_once('=') {
            let key = k.trim();
            let val = v.trim();
            match key {
                "out_time_ms" => {
                    // ffmpeg -progress: out_time_ms 实际单位为微秒
                    progress.out_time_ms = val.parse::<u64>().ok().map(|us| us / 1000);
                }
                "total_size" => {
                    progress.total_size = val.parse::<u64>().ok();
                }
                "speed" => {
                    progress.speed = Some(val.to_string());
                }
                "progress" => {
                    progress.progress = Some(val.to_string());
                    if let Some(sink) = progress_sink.as_ref() {
                        sink(progress.clone());
                    }
                }
                _ => {}
            }
        } else {
            if tail.len() < 200 {
                tail.push(line);
            } else {
                tail.rotate_left(1);
                if let Some(last) = tail.last_mut() {
                    *last = line;
                }
            }
        }
    }

    let status = child.wait().await.change_context(AppError::Unknown)?;
    if !status.success() {
        let stderr = if tail.is_empty() {
            "-".to_string()
        } else {
            tail.join("\n")
        };
        return Err(AppError::Custom(format!("ffmpeg 执行失败: {stderr}")).into());
    }
    Ok(())
}

impl ProbeSummary {
    async fn with_size_from_fs(mut self, path: &Path) -> Self {
        if self.size_bytes == 0 {
            if let Ok(metadata) = fs::metadata(path).await {
                self.size_bytes = metadata.len();
            }
        }
        if self.overall_bps.is_none() && self.duration_sec > 1.0 && self.size_bytes > 0 {
            let bps = (self.size_bytes as f64 * 8.0 / self.duration_sec).round() as u64;
            if bps > 0 {
                self.overall_bps = Some(bps);
            }
        }
        self
    }
}

fn decide_audio_plan(input: &ProbeSummary, allow_copy: bool) -> AudioPlan {
    let codec = input.audio_codec.as_deref().unwrap_or_default();
    if allow_copy && codec == "aac" {
        return AudioPlan::Copy;
    }
    if input.audio_codec.is_none() {
        return AudioPlan::None;
    }
    let guessed = input.audio_bps.unwrap_or(160_000);
    let clamped = guessed.clamp(96_000, 320_000);
    AudioPlan::Aac { bps: clamped }
}

fn decide_target_bitrates(
    input: &ProbeSummary,
    audio: &AudioPlan,
    video_factor: f64,
) -> (u64, Option<u64>) {
    let duration = input.duration_sec.max(1.0);
    let overall_bps = input.overall_bps.unwrap_or_else(|| {
        if input.size_bytes == 0 {
            5_000_000
        } else {
            (input.size_bytes as f64 * 8.0 / duration).round() as u64
        }
    });

    let audio_bps_for_total = match audio {
        AudioPlan::Copy => input.audio_bps.unwrap_or(160_000),
        AudioPlan::Aac { bps } => *bps,
        AudioPlan::None => 0,
    };

    let base_video_bps = input
        .video_bps
        .filter(|v| *v > 0)
        .unwrap_or_else(|| overall_bps.saturating_sub(audio_bps_for_total));
    let base_video_bps = base_video_bps.clamp(200_000, 120_000_000);
    let target_video_bps = ((base_video_bps as f64) * video_factor).round() as u64;
    let target_video_bps = target_video_bps.clamp(200_000, 120_000_000);

    let target_audio_bps = match audio {
        AudioPlan::Aac { bps } => Some(*bps),
        _ => None,
    };
    (target_video_bps, target_audio_bps)
}

fn push_audio_args(args: &mut Vec<String>, plan: &AudioPlan, target_audio_bps: Option<u64>) {
    match plan {
        AudioPlan::Copy => {
            args.push("-c:a".to_string());
            args.push("copy".to_string());
        }
        AudioPlan::Aac { .. } => {
            args.push("-c:a".to_string());
            args.push("aac".to_string());
            if let Some(bps) = target_audio_bps {
                args.push("-b:a".to_string());
                args.push(format!("{bps}"));
            }
        }
        AudioPlan::None => {}
    }
}

fn build_effect_windows(video_id: &str, width: u32, height: u32, duration_sec: f64) -> Vec<EffectWindow> {
    let mut hasher = DefaultHasher::new();
    video_id.hash(&mut hasher);
    width.hash(&mut hasher);
    height.hash(&mut hasher);
    duration_sec.to_bits().hash(&mut hasher);
    let seed = hasher.finish();
    let mut rng = StdRng::seed_from_u64(seed);

    let dots = rng.gen_range(EFFECT_WINDOW_COUNT_MIN..=EFFECT_WINDOW_COUNT_MAX);
    let max_start = (duration_sec * 0.95).max(0.6);

    let mut windows = Vec::new();

    for _ in 0..dots {
        let max_w = (width / 40).max(6);
        let min_w = (width / 160).max(2);
        let max_h = (height / 40).max(6);
        let min_h = (height / 160).max(2);
        let box_w = rng.gen_range(min_w..=max_w).min(width.saturating_sub(1));
        let box_h = rng.gen_range(min_h..=max_h).min(height.saturating_sub(1));
        let x_max = width.saturating_sub(box_w).max(1);
        let y_max = height.saturating_sub(box_h).max(1);
        let x = rng.gen_range(0..x_max);
        let y = rng.gen_range(0..y_max);
        let span = rng.gen_range(EFFECT_WINDOW_SPAN_MIN_SEC..=EFFECT_WINDOW_SPAN_MAX_SEC);
        let start = rng.gen_range(0.2..max_start).min((duration_sec - span - 0.05).max(0.0));
        let end = (start + span).min(duration_sec - 0.05);
        windows.push(EffectWindow {
            start_sec: start.max(0.0),
            end_sec: end.max(start + 0.05),
            x,
            y,
            width: box_w,
            height: box_h,
            alpha: 0.35,
        });
    }
    windows.sort_by(|a, b| a.start_sec.total_cmp(&b.start_sec));
    windows
}

fn build_effect_filter(windows: &[EffectWindow], segment_start_sec: f64, segment_end_sec: f64) -> String {
    let mut filters = Vec::new();
    for window in windows {
        let overlap_start = window.start_sec.max(segment_start_sec);
        let overlap_end = window.end_sec.min(segment_end_sec);
        if overlap_end - overlap_start <= TIME_EPSILON_SEC {
            continue;
        }
        let local_start = (overlap_start - segment_start_sec).max(0.0);
        let local_end = (overlap_end - segment_start_sec).max(local_start + 0.02);
        filters.push(format!(
            "drawbox=x={}:y={}:w={}:h={}:color=black@{:.2}:t=fill:enable='between(t,{:.3},{:.3})'",
            window.x,
            window.y,
            window.width,
            window.height,
            window.alpha,
            local_start,
            local_end
        ));
    }
    filters.join(",")
}

fn build_segment_plan(duration_sec: f64, keyframes: &[f64], windows: &[EffectWindow]) -> Vec<SegmentPlan> {
    let mut boundaries = keyframes.to_vec();
    boundaries.push(0.0);
    boundaries.push(duration_sec.max(0.0));
    boundaries.sort_by(|a, b| a.total_cmp(b));
    boundaries.dedup_by(|a, b| (*a - *b).abs() <= TIME_EPSILON_SEC);

    let mut affected_segments: Vec<SegmentPlan> = windows
        .iter()
        .map(|window| {
            let start_sec = find_prev_boundary(&boundaries, window.start_sec);
            let end_sec = find_next_boundary(&boundaries, window.end_sec);
            SegmentPlan {
                start_sec,
                end_sec,
                kind: SegmentPlanKind::Reencode {
                    windows: vec![window.clone()],
                },
            }
        })
        .collect();
    affected_segments.sort_by(|a, b| a.start_sec.total_cmp(&b.start_sec));

    let mut merged: Vec<SegmentPlan> = Vec::new();
    for segment in affected_segments {
        match merged.last_mut() {
            Some(last)
                if segment.start_sec <= last.end_sec + TIME_EPSILON_SEC
                    && matches!(last.kind, SegmentPlanKind::Reencode { .. }) =>
            {
                last.end_sec = last.end_sec.max(segment.end_sec);
                if let (SegmentPlanKind::Reencode { windows: lhs }, SegmentPlanKind::Reencode { windows: rhs }) =
                    (&mut last.kind, segment.kind)
                {
                    lhs.extend(rhs);
                    lhs.sort_by(|a, b| a.start_sec.total_cmp(&b.start_sec));
                }
            }
            _ => merged.push(segment),
        }
    }

    let mut plan = Vec::new();
    let mut cursor = 0.0;
    for segment in merged {
        if segment.start_sec > cursor + TIME_EPSILON_SEC {
            plan.push(SegmentPlan {
                start_sec: cursor,
                end_sec: segment.start_sec,
                kind: SegmentPlanKind::Copy,
            });
        }
        plan.push(segment.clone());
        cursor = segment.end_sec;
    }
    if duration_sec > cursor + TIME_EPSILON_SEC {
        plan.push(SegmentPlan {
            start_sec: cursor,
            end_sec: duration_sec,
            kind: SegmentPlanKind::Copy,
        });
    }
    plan.retain(|segment| segment.end_sec - segment.start_sec > TIME_EPSILON_SEC);
    plan
}

fn find_prev_boundary(boundaries: &[f64], target: f64) -> f64 {
    boundaries
        .iter()
        .copied()
        .take_while(|point| *point <= target + TIME_EPSILON_SEC)
        .last()
        .unwrap_or(0.0)
}

fn find_next_boundary(boundaries: &[f64], target: f64) -> f64 {
    boundaries
        .iter()
        .copied()
        .find(|point| *point + TIME_EPSILON_SEC >= target)
        .unwrap_or_else(|| boundaries.last().copied().unwrap_or(target))
}

async fn export_copy_segment_to_ts(
    input: &Path,
    start_sec: f64,
    end_sec: f64,
    output: &Path,
) -> AppResult<()> {
    let duration = (end_sec - start_sec).max(0.01);
    let args = vec![
        "-y".to_string(),
        "-v".to_string(),
        "error".to_string(),
        "-ss".to_string(),
        format_time_arg(start_sec),
        "-t".to_string(),
        format_time_arg(duration),
        "-i".to_string(),
        input.to_string_lossy().to_string(),
        "-map".to_string(),
        "0:v:0".to_string(),
        "-map".to_string(),
        "0:a:0?".to_string(),
        "-c".to_string(),
        "copy".to_string(),
        "-copyinkf".to_string(),
        "-bsf:v".to_string(),
        "h264_mp4toannexb".to_string(),
        "-muxpreload".to_string(),
        "0".to_string(),
        "-muxdelay".to_string(),
        "0".to_string(),
        "-f".to_string(),
        "mpegts".to_string(),
        output.to_string_lossy().to_string(),
    ];
    run_ffmpeg_simple(&args).await
}

async fn export_effect_segment_to_ts(
    input: &Path,
    segment: &SegmentPlan,
    windows: &[EffectWindow],
    output: &Path,
    audio_plan: &AudioPlan,
    target_video_bps: u64,
    target_audio_bps: Option<u64>,
    progress_sink: Option<&ProgressSink>,
    total_duration_ms: u64,
) -> AppResult<()> {
    let duration = (segment.end_sec - segment.start_sec).max(0.01);
    let filter = build_effect_filter(windows, segment.start_sec, segment.end_sec);
    let mut args = vec![
        "-y".to_string(),
        "-progress".to_string(),
        "pipe:2".to_string(),
        "-nostats".to_string(),
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-ss".to_string(),
        format_time_arg(segment.start_sec),
        "-t".to_string(),
        format_time_arg(duration),
        "-i".to_string(),
        input.to_string_lossy().to_string(),
        "-map".to_string(),
        "0:v:0".to_string(),
        "-map".to_string(),
        "0:a:0?".to_string(),
        "-vf".to_string(),
        filter,
        "-c:v".to_string(),
        "libx264".to_string(),
        "-preset".to_string(),
        EFFECT_PRESET.to_string(),
        "-b:v".to_string(),
        format!("{target_video_bps}"),
        "-maxrate".to_string(),
        format!("{}", (target_video_bps as f64 * 1.15).round() as u64),
        "-bufsize".to_string(),
        format!("{}", target_video_bps.saturating_mul(2)),
        "-pix_fmt".to_string(),
        "yuv420p".to_string(),
    ];
    push_audio_args(&mut args, audio_plan, target_audio_bps);
    args.extend([
        "-muxpreload".to_string(),
        "0".to_string(),
        "-muxdelay".to_string(),
        "0".to_string(),
        "-f".to_string(),
        "mpegts".to_string(),
        output.to_string_lossy().to_string(),
    ]);

    let segment_offset_ms = (segment.start_sec * 1000.0).round().max(0.0) as u64;
    let segment_duration_ms = ((segment.end_sec - segment.start_sec) * 1000.0)
        .round()
        .max(1.0) as u64;
    let mapped_sink = progress_sink.map(|outer| {
        let outer = outer.clone();
        let mut initial = FfmpegProgress::default();
        initial.out_time_ms = Some(segment_offset_ms.min(total_duration_ms));
        initial.progress = Some("continue".to_string());
        outer(initial);
        Arc::new(move |progress: FfmpegProgress| {
            let mut mapped = progress.clone();
            let local_ms = progress.out_time_ms.unwrap_or(0).min(segment_duration_ms);
            mapped.out_time_ms = Some((segment_offset_ms + local_ms).min(total_duration_ms));
            outer(mapped);
        }) as ProgressSink
    });
    run_ffmpeg_with_progress(&args, mapped_sink)
        .await
        .change_context(AppError::Custom(
            "执行局部黑点视频处理失败，请确认已安装 ffmpeg".to_string(),
        ))
}

async fn concat_segments_to_mp4(
    segment_paths: &[PathBuf],
    output: &Path,
    has_audio: bool,
    progress_sink: Option<&ProgressSink>,
    total_duration_ms: u64,
) -> AppResult<()> {
    let list_path = output
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(
            ".{}.concat.txt",
            output
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("output")
        ));
    let mut list_content = String::new();
    for path in segment_paths {
        let absolute = path
            .canonicalize()
            .change_context(AppError::Custom("解析分段文件绝对路径失败".to_string()))?;
        list_content.push_str("file ");
        list_content.push('\'');
        list_content.push_str(&absolute.to_string_lossy().replace('\'', "'\\''"));
        list_content.push_str("'\n");
    }
    let mut file = fs::File::create(&list_path)
        .await
        .change_context(AppError::Custom("创建 concat 列表失败".to_string()))?;
    file.write_all(list_content.as_bytes())
        .await
        .change_context(AppError::Custom("写入 concat 列表失败".to_string()))?;
    file.flush()
        .await
        .change_context(AppError::Custom("刷新 concat 列表失败".to_string()))?;

    let mut args = vec![
        "-y".to_string(),
        "-v".to_string(),
        "error".to_string(),
        "-f".to_string(),
        "concat".to_string(),
        "-safe".to_string(),
        "0".to_string(),
        "-i".to_string(),
        list_path.to_string_lossy().to_string(),
        "-c".to_string(),
        "copy".to_string(),
    ];
    if has_audio {
        args.push("-bsf:a".to_string());
        args.push("aac_adtstoasc".to_string());
    }
    args.extend([
        "-movflags".to_string(),
        "+faststart".to_string(),
        output.to_string_lossy().to_string(),
    ]);
    let result = run_ffmpeg_simple(&args).await;
    if let Err(err) = fs::remove_file(&list_path).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        eprintln!("cleanup concat list failed: {}", err);
    }
    result?;
    emit_progress_checkpoint(progress_sink, total_duration_ms);
    Ok(())
}

async fn run_ffmpeg_simple(args: &[String]) -> AppResult<()> {
    let output = Command::new("ffmpeg")
        .kill_on_drop(true)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .change_context(AppError::Unknown)?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(AppError::Custom(format!("ffmpeg 执行失败: {stderr}")).into());
    }
    Ok(())
}

fn emit_progress_checkpoint(progress_sink: Option<&ProgressSink>, out_time_ms: u64) {
    if let Some(sink) = progress_sink {
        sink(FfmpegProgress {
            out_time_ms: Some(out_time_ms),
            progress: Some("continue".to_string()),
            ..Default::default()
        });
    }
}

fn format_time_arg(seconds: f64) -> String {
    format!("{:.6}", seconds.max(0.0))
}

#[cfg(test)]
mod tests {
    use super::{build_effect_filter, build_effect_windows, build_segment_plan, need_transcode, EffectWindow, ProbeSummary, SegmentPlanKind};

    #[test]
    fn no_transcode_when_bilibili_friendly() {
        let summary = ProbeSummary {
            format_name: "mov,mp4".to_string(),
            duration_sec: 60.0,
            size_bytes: 1024,
            overall_bps: None,
            video_codec: Some("h264".to_string()),
            video_pix_fmt: Some("yuv420p".to_string()),
            width: Some(1920),
            height: Some(1080),
            video_bps: None,
            audio_codec: Some("aac".to_string()),
            audio_bps: None,
        };
        assert!(!need_transcode(&summary));
    }

    #[test]
    fn transcode_when_video_codec_not_h264() {
        let summary = ProbeSummary {
            format_name: "mp4".to_string(),
            duration_sec: 60.0,
            size_bytes: 1024,
            overall_bps: None,
            video_codec: Some("hevc".to_string()),
            video_pix_fmt: Some("yuv420p".to_string()),
            width: Some(1920),
            height: Some(1080),
            video_bps: None,
            audio_codec: Some("aac".to_string()),
            audio_bps: None,
        };
        assert!(need_transcode(&summary));
    }

    #[test]
    fn no_audio_is_allowed() {
        let summary = ProbeSummary {
            format_name: "matroska,webm".to_string(),
            duration_sec: 60.0,
            size_bytes: 1024,
            overall_bps: None,
            video_codec: Some("h264".to_string()),
            video_pix_fmt: Some("yuv420p".to_string()),
            width: Some(1920),
            height: Some(1080),
            video_bps: None,
            audio_codec: None,
            audio_bps: None,
        };
        assert!(!need_transcode(&summary));
    }

    #[test]
    fn effect_filter_contains_drawbox() {
        let windows = build_effect_windows("video1", 1920, 1080, 210.0);
        let filter = build_effect_filter(&windows, 0.0, 210.0);
        assert!(filter.contains("drawbox="));
    }

    #[test]
    fn segment_plan_merges_overlapping_gops() {
        let windows = vec![
            EffectWindow {
                start_sec: 2.1,
                end_sec: 2.2,
                x: 10,
                y: 10,
                width: 10,
                height: 10,
                alpha: 0.35,
            },
            EffectWindow {
                start_sec: 2.3,
                end_sec: 2.4,
                x: 20,
                y: 20,
                width: 10,
                height: 10,
                alpha: 0.35,
            },
        ];
        let plan = build_segment_plan(8.0, &[0.0, 2.0, 4.0, 6.0, 8.0], &windows);
        assert_eq!(plan.len(), 3);
        assert!(matches!(plan[1].kind, SegmentPlanKind::Reencode { .. }));
        assert!((plan[1].start_sec - 2.0).abs() < 0.001);
        assert!((plan[1].end_sec - 4.0).abs() < 0.001);
    }
}
