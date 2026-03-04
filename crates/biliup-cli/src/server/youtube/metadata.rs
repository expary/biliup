use crate::server::config::Config;
use crate::server::errors::{AppError, AppResult};
use crate::server::common::util::normalize_proxy;
use crate::server::youtube::collector::VideoMetadata;
use error_stack::ResultExt;
use serde::Deserialize;
use serde_json::json;

const DEEPSEEK_CHAT_URL: &str = "https://api.deepseek.com/chat/completions";

const SYSTEM_PROMPT: &str = "你是 B 站视频运营编辑。请严格输出 JSON，键必须是 title、description、tags。\
核心目标：在不编造、不夸张的前提下，生成更有吸引力、可发布的中文元数据，不能只做直译。\
title 要求：简体中文、无 emoji、20-40 字优先、绝不超过 80 字；保留专有名词与关键信息，可适度润色增强点击意愿。\
description 要求：仅中文重写，重点提炼看点与价值，不要保留英文原文，不要写 markdown 标题。\
tags 要求：6-10 个中文标签，每个 <=20 字，无 emoji，尽量覆盖主题、人物、场景与内容类型。\
禁止出现“搬运”“转载”等词。";
const FORBIDDEN_KEYWORDS: &[&str] = &["搬运", "转载", "转自", "转发", "二传"];

#[derive(Debug, Clone)]
pub struct GeneratedMetadata {
    pub title: String,
    pub description: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct DescriptionTailPolicy {
    pub is_self_made: bool,
    pub include_source_link: bool,
    pub include_source_channel: bool,
}

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

#[derive(Debug, Deserialize)]
struct GeneratedMetadataRaw {
    title: String,
    description: String,
    tags: Vec<String>,
}

pub async fn generate_metadata(
    config: &Config,
    source_title: &str,
    source_description: &str,
    source_tags: &[String],
    source_url: &str,
    channel_name: Option<&str>,
    tail_policy: DescriptionTailPolicy,
) -> AppResult<GeneratedMetadata> {
    let api_key = config
        .deepseek_api_key
        .clone()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            std::env::var("DEEPSEEK_API_KEY")
                .ok()
                .filter(|v| !v.trim().is_empty())
        })
        .ok_or_else(|| AppError::Custom("未配置 DEEPSEEK_API_KEY".to_string()))?;

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

    let tags_joined = if source_tags.is_empty() {
        "无".to_string()
    } else {
        source_tags.join("、")
    };
    let user_prompt = format!(
        "请基于以下信息，产出适合 B 站投稿的元数据。\
\n要求：标题要更吸引人但不能标题党；不要逐字翻译；不得虚构事实。\
\n要求：不要出现“搬运”“转载”等词。\
\n\n原标题：{source_title}\n原简介：{source_description}\n原标签：{tags_joined}\n\
\n请直接输出 JSON：{{\"title\":\"...\",\"description\":\"...\",\"tags\":[\"...\",\"...\"]}}"
    );
    let payload = json!({
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.4
    });

    let mut client_builder = reqwest::Client::builder();
    if let Some(proxy) = normalize_proxy(config.proxy.as_deref()) {
        let proxy = reqwest::Proxy::all(&proxy)
            .change_context(AppError::Custom("代理配置格式错误".to_string()))?;
        client_builder = client_builder.proxy(proxy);
    }
    let client = client_builder
        .build()
        .change_context(AppError::Custom("创建 HTTP 客户端失败".to_string()))?;

    let response = client
        .post(api_base)
        .bearer_auth(api_key)
        .json(&payload)
        .send()
        .await
        .change_context(AppError::Custom("请求 DeepSeek 失败".to_string()))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::Custom(format!("DeepSeek 响应错误: {status} {text}")).into());
    }

    let parsed: DeepSeekChatResponse = response
        .json()
        .await
        .change_context(AppError::Custom("DeepSeek 返回格式解析失败".to_string()))?;
    let content = parsed
        .choices
        .first()
        .and_then(|choice| choice.message.content.as_deref())
        .ok_or_else(|| AppError::Custom("DeepSeek 返回内容为空".to_string()))?;

    let raw = parse_generated_json(content)?;

    let title = generate_title_with_fallback(&raw.title, source_title);

    let mut description = sanitize_description(&raw.description);
    if let Some(tail) = build_description_tail(source_url, channel_name, tail_policy) {
        description.push_str("\n\n");
        description.push_str(&tail);
    }
    description = sanitize_description(&description);
    if description.is_empty() {
        return Err(AppError::Custom("DeepSeek 返回简介为空".to_string()).into());
    }

    let tags = sanitize_tags(raw.tags, source_tags);
    if tags.is_empty() {
        return Err(AppError::Custom("DeepSeek 返回标签为空".to_string()).into());
    }

    Ok(GeneratedMetadata {
        title,
        description,
        tags,
    })
}

fn build_description_tail(
    source_url: &str,
    channel_name: Option<&str>,
    policy: DescriptionTailPolicy,
) -> Option<String> {
    if policy.is_self_made {
        let mut lines = Vec::new();
        if policy.include_source_link {
            lines.push(format!("来源链接：{source_url}"));
        }
        if policy.include_source_channel {
            lines.push(format!("来源频道：{}", channel_name.unwrap_or("未知频道")));
        }
        if lines.is_empty() {
            return None;
        }
        return Some(lines.join("\n"));
    }

    Some(format!(
        "来源链接：{source_url}\n来源频道：{}\n说明：内容经整理后发布至哔哩哔哩。",
        channel_name.unwrap_or("未知频道")
    ))
}

fn generate_title_with_fallback(generated_title: &str, source_title: &str) -> String {
    let generated = sanitize_title(generated_title);
    if !generated.is_empty() && contains_cjk(&generated) {
        return truncate_chars(&generated, 80);
    }

    let fallback_source = sanitize_title(source_title);
    if !fallback_source.is_empty() && contains_cjk(&fallback_source) {
        return truncate_chars(&fallback_source, 80);
    }

    "精选视频内容分享".to_string()
}

fn parse_generated_json(content: &str) -> AppResult<GeneratedMetadataRaw> {
    if let Ok(raw) = serde_json::from_str::<GeneratedMetadataRaw>(content.trim()) {
        return Ok(raw);
    }

    let start = content.find('{');
    let end = content.rfind('}');
    let Some((start, end)) = start.zip(end) else {
        return Err(AppError::Custom("DeepSeek 未返回 JSON".to_string()).into());
    };
    let json_part = &content[start..=end];
    serde_json::from_str::<GeneratedMetadataRaw>(json_part)
        .change_context(AppError::Custom("DeepSeek JSON 解析失败".to_string()))
}

pub fn sanitize_title(raw: &str) -> String {
    truncate_chars(
        &remove_forbidden_keywords(&strip_emoji(raw))
            .replace('\r', " ")
            .replace('\n', " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
        80,
    )
    .trim()
    .to_string()
}

pub fn sanitize_description(raw: &str) -> String {
    remove_forbidden_keywords(&strip_emoji(raw))
        .replace('\r', "\n")
        .replace("\n\n\n", "\n\n")
        .trim()
        .to_string()
}

pub fn sanitize_tags(raw_tags: Vec<String>, source_tags: &[String]) -> Vec<String> {
    let mut tags = raw_tags
        .into_iter()
        .chain(source_tags.iter().cloned())
        .map(|tag| sanitize_tag_text(&tag))
        .filter(|tag| !tag.is_empty())
        .collect::<Vec<_>>();

    tags.extend([
        "中文解读".to_string(),
        "内容分享".to_string(),
        "精选视频".to_string(),
        "知识分享".to_string(),
        "哔哩哔哩".to_string(),
    ]);

    let mut dedup = Vec::new();
    for tag in tags {
        if !contains_cjk(&tag) {
            continue;
        }
        if contains_forbidden_keyword(&tag) {
            continue;
        }
        if dedup.iter().any(|x: &String| x == &tag) {
            continue;
        }
        dedup.push(tag);
        if dedup.len() >= 10 {
            break;
        }
    }

    if dedup.len() < 6 {
        let defaults = [
            "中文解读",
            "内容分享",
            "精选视频",
            "实用技巧",
            "推荐观看",
            "哔哩哔哩",
        ];
        for tag in defaults {
            let tag = tag.to_string();
            if dedup.iter().any(|x| x == &tag) {
                continue;
            }
            dedup.push(tag);
            if dedup.len() >= 6 {
                break;
            }
        }
    }
    dedup
}

pub fn sanitize_submit_tags(raw_tags: Vec<String>) -> Vec<String> {
    let mut dedup = Vec::new();
    for raw_tag in raw_tags {
        let tag = sanitize_tag_text(&raw_tag);
        if tag.is_empty() || contains_forbidden_keyword(&tag) || !contains_cjk(&tag) {
            continue;
        }
        if dedup.iter().any(|x| x == &tag) {
            continue;
        }
        dedup.push(tag);
        if dedup.len() >= 10 {
            break;
        }
    }
    if dedup.is_empty() {
        dedup.push("内容分享".to_string());
    }
    dedup
}

pub fn truncate_chars(input: &str, max_len: usize) -> String {
    input.chars().take(max_len).collect()
}

pub fn strip_emoji(input: &str) -> String {
    input.chars().filter(|ch| !is_emoji(*ch)).collect()
}

fn is_emoji(ch: char) -> bool {
    let code = ch as u32;
    matches!(
        code,
        0x1F300..=0x1FAFF
            | 0x2600..=0x27BF
            | 0xFE00..=0xFE0F
            | 0x1F1E6..=0x1F1FF
            | 0x200D
    )
}

pub fn contains_cjk(input: &str) -> bool {
    input
        .chars()
        .any(|ch| matches!(ch as u32, 0x4E00..=0x9FFF | 0x3400..=0x4DBF))
}

fn sanitize_tag_text(raw: &str) -> String {
    let cleaned = remove_forbidden_keywords(&strip_emoji(raw));
    truncate_chars(cleaned.trim(), 20).trim().to_string()
}

fn contains_forbidden_keyword(input: &str) -> bool {
    FORBIDDEN_KEYWORDS
        .iter()
        .any(|keyword| !keyword.is_empty() && input.contains(keyword))
}

fn remove_forbidden_keywords(input: &str) -> String {
    FORBIDDEN_KEYWORDS
        .iter()
        .fold(input.to_string(), |acc, keyword| acc.replace(keyword, ""))
}

pub fn metadata_from_source(source: &VideoMetadata) -> (String, String, Vec<String>) {
    (
        source.title.clone().unwrap_or_default(),
        source.description.clone().unwrap_or_default(),
        source.tags.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_title_strips_emoji_and_limits_to_80() {
        let input = format!("标题😀{}\n", "测".repeat(120));
        let output = sanitize_title(&input);
        assert!(!output.contains('😀'));
        assert!(output.chars().count() <= 80);
    }

    #[test]
    fn title_fallback_uses_source_when_generated_empty() {
        let output = generate_title_with_fallback("😀😀😀", "这是备用中文标题");
        assert_eq!(output, "这是备用中文标题");
    }

    #[test]
    fn title_fallback_uses_default_when_no_chinese() {
        let output = generate_title_with_fallback("english title", "backup title");
        assert_eq!(output, "精选视频内容分享");
    }

    #[test]
    fn sanitize_tags_dedup_and_limit() {
        let raw_tags = vec![
            "科技".to_string(),
            "科技".to_string(),
            "AI😀".to_string(),
            "视频搬运".to_string(),
            "这是一个超过二十个字符的超长标签用于测试".to_string(),
            "游戏".to_string(),
            "教程".to_string(),
            "开箱".to_string(),
            "评测".to_string(),
            "更新".to_string(),
            "速览".to_string(),
            "热点".to_string(),
            "新闻".to_string(),
        ];
        let source_tags = vec!["深度解读".to_string()];
        let tags = sanitize_tags(raw_tags, &source_tags);
        assert!((6..=10).contains(&tags.len()));
        assert!(tags.iter().all(|tag| tag.chars().count() <= 20));
        assert!(tags.iter().all(|tag| contains_cjk(tag)));
        assert!(tags.iter().all(|tag| !tag.contains("搬运")));
        let mut dedup = tags.clone();
        dedup.sort();
        dedup.dedup();
        assert_eq!(dedup.len(), tags.len());
    }

    #[test]
    fn self_made_tail_without_declaration() {
        let tail = build_description_tail(
            "https://example.com/v",
            Some("示例频道"),
            DescriptionTailPolicy {
                is_self_made: true,
                include_source_link: true,
                include_source_channel: true,
            },
        )
        .expect("tail");
        assert!(tail.contains("来源链接"));
        assert!(tail.contains("来源频道"));
        assert!(!tail.contains("声明"));
    }

    #[test]
    fn sanitize_title_and_description_remove_forbidden_keywords() {
        let title = sanitize_title("高能搬运：原片转载精选");
        let description = sanitize_description("这是搬运内容\n已转载到本平台");
        assert!(!title.contains("搬运"));
        assert!(!title.contains("转载"));
        assert!(!description.contains("搬运"));
        assert!(!description.contains("转载"));
    }

    #[test]
    fn sanitize_submit_tags_filters_forbidden_keywords() {
        let tags = sanitize_submit_tags(vec![
            "搬运精选".to_string(),
            "转载解读".to_string(),
            "技术分享".to_string(),
            "实战教程".to_string(),
        ]);
        assert!(tags.iter().all(|tag| !tag.contains("搬运")));
        assert!(tags.iter().all(|tag| !tag.contains("转载")));
        assert!(tags.iter().any(|tag| tag == "技术分享"));
    }

    #[test]
    fn self_made_tail_can_be_disabled() {
        let tail = build_description_tail(
            "https://example.com/v",
            Some("示例频道"),
            DescriptionTailPolicy {
                is_self_made: true,
                include_source_link: false,
                include_source_channel: false,
            },
        );
        assert!(tail.is_none());
    }

    #[test]
    fn repost_tail_without_forbidden_keywords() {
        let tail = build_description_tail(
            "https://example.com/v",
            Some("示例频道"),
            DescriptionTailPolicy {
                is_self_made: false,
                include_source_link: true,
                include_source_channel: true,
            },
        )
        .expect("tail");
        assert!(!tail.contains("搬运"));
        assert!(!tail.contains("转载"));
    }
}
