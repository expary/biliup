use crate::server::errors::{AppError, AppResult};
use error_stack::ResultExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::fs;
use tokio::process::Command;

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

pub async fn transcode_with_report(video_id: &str, input: &Path) -> AppResult<FfmpegReport> {
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
    let result = Command::new("ffmpeg")
        .kill_on_drop(true)
        .args(&args)
        .output()
        .await
        .change_context(AppError::Custom(
            "执行 ffmpeg 转码失败，请确认已安装 ffmpeg".to_string(),
        ))?;

    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        return Err(AppError::Custom(format!("ffmpeg 转码失败: {stderr}")).into());
    }

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

pub async fn apply_upload_effects_with_report(video_id: &str, input: &Path) -> AppResult<FfmpegReport> {
    let input_summary = probe_summary(input).await?;
    let (width, height, duration_sec) = (input_summary.width.unwrap_or(1920), input_summary.height.unwrap_or(1080), input_summary.duration_sec.max(5.0));
    let filter = build_effect_filter(video_id, width, height, duration_sec);
    let output = input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{video_id}.fx.mp4"));

    let audio_plan = decide_audio_plan(&input_summary, true);
    let (target_video_bps, target_audio_bps) = decide_target_bitrates(&input_summary, &audio_plan, 1.02);

    let preset = "slow";
    let mut args = vec![
        "-y".to_string(),
        "-i".to_string(),
        input.to_string_lossy().to_string(),
        "-map".to_string(),
        "0:v:0".to_string(),
        "-map".to_string(),
        "0:a:0?".to_string(),
        "-vf".to_string(),
        filter.clone(),
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
    let result = Command::new("ffmpeg")
        .kill_on_drop(true)
        .args(&args)
        .output()
        .await
        .change_context(AppError::Custom(
            "执行 ffmpeg 视频处理失败，请确认已安装 ffmpeg".to_string(),
        ))?;

    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        return Err(AppError::Custom(format!("ffmpeg 视频处理失败: {stderr}")).into());
    }

    let output_summary = probe_summary(&output).await?;
    Ok(FfmpegReport {
        output,
        input: input_summary,
        output_summary,
        elapsed_ms: start.elapsed().as_millis(),
        args,
        target_video_bps,
        audio_plan,
        filter: Some(filter),
    })
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

fn build_effect_filter(video_id: &str, width: u32, height: u32, duration_sec: f64) -> String {
    let mut hasher = DefaultHasher::new();
    video_id.hash(&mut hasher);
    width.hash(&mut hasher);
    height.hash(&mut hasher);
    duration_sec.to_bits().hash(&mut hasher);
    let seed = hasher.finish();
    let mut rng = StdRng::seed_from_u64(seed);

    let dots = rng.gen_range(2..=4);

    let mut filters = Vec::new();

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
        let start = rng.gen_range(0.2..(duration_sec * 0.92).max(0.3));
        let span = rng.gen_range(0.2..0.9);
        let end = (start + span).min(duration_sec - 0.05);
        filters.push(format!(
            "drawbox=x={x}:y={y}:w={box_w}:h={box_h}:color=black@0.35:t=fill:enable='between(t,{start:.2},{end:.2})'"
        ));
    }

    filters.join(",")
}

#[cfg(test)]
mod tests {
    use super::{build_effect_filter, need_transcode, ProbeSummary};

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
        let filter = build_effect_filter("video1", 1920, 1080, 210.0);
        assert!(filter.contains("drawbox="));
    }
}
