use axum::http::{StatusCode, Uri, header};
use axum::response::{Html, IntoResponse, Response};
use rust_embed::Embed;
use std::borrow::Cow;

/// 默认首页文件名
static INDEX_HTML: &str = "index.html";
static YOUTUBE_DYNAMIC_FALLBACK_HTML: &str = "youtube/0.html";
static YOUTUBE_DYNAMIC_FALLBACK_TXT: &str = "youtube/0.txt";

/// 嵌入的静态资源
#[derive(Embed)]
#[folder = "../../out/"]
struct Assets;

fn youtube_dynamic_fallback(path: &str) -> Option<&'static str> {
    let rest = path.strip_prefix("youtube/")?;
    if rest.is_empty() || rest.contains('/') {
        return None;
    }
    if let Some((id, ext)) = rest.split_once('.') {
        if id.is_empty() || !id.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        match ext {
            "html" => Some(YOUTUBE_DYNAMIC_FALLBACK_HTML),
            "txt" => Some(YOUTUBE_DYNAMIC_FALLBACK_TXT),
            _ => None,
        }
    } else {
        if !rest.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        Some(YOUTUBE_DYNAMIC_FALLBACK_HTML)
    }
}

fn normalize_asset_lookup_path(path: &str) -> Cow<'_, str> {
    let mut should_allocate = false;
    for pattern in ["%5B", "%5D", "%5b", "%5d"] {
        if path.contains(pattern) {
            should_allocate = true;
            break;
        }
    }
    if !should_allocate {
        return Cow::Borrowed(path);
    }
    Cow::Owned(
        path.replace("%5B", "[")
            .replace("%5D", "]")
            .replace("%5b", "[")
            .replace("%5d", "]"),
    )
}

/// 静态文件处理器，用于服务单页应用
pub async fn static_handler(uri: Uri) -> impl IntoResponse {
    let raw_path = uri.path().trim_start_matches('/');
    let path = normalize_asset_lookup_path(raw_path);

    // 根路径或直接访问index.html时返回首页
    if path.is_empty() || path.as_ref() == INDEX_HTML {
        return index_html().await;
    }

    // 尝试查找对应的HTML文件
    let guess_html = path.as_ref().to_owned() + ".html";
    if let Some(html) = Assets::get(&guess_html) {
        return Html(html.data).into_response();
    }

    // 查找静态资源文件
    match Assets::get(path.as_ref()) {
        Some(content) => {
            // 根据文件扩展名推断MIME类型
            let mime = mime_guess::from_path(path.as_ref()).first_or_octet_stream();
            ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response()
        }
        None => {
            if let Some(fallback) = youtube_dynamic_fallback(path.as_ref()) {
                if let Some(content) = Assets::get(fallback) {
                    if fallback.ends_with(".html") {
                        return Html(content.data).into_response();
                    }
                    let mime = mime_guess::from_path(fallback).first_or_octet_stream();
                    return ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response();
                }
            }

            // 对于前端路由（无扩展名），回退到首页交由客户端路由处理
            // 静态资源缺失（如 .js/.css/.png）仍返回 404，避免吞掉真实错误
            if path.as_ref().contains('.') {
                not_found().await
            } else {
                index_html().await
            }
        }
    }
}

/// 返回首页HTML内容
async fn index_html() -> Response {
    match Assets::get(INDEX_HTML) {
        Some(content) => Html(content.data).into_response(),
        None => not_found().await,
    }
}

/// 返回404错误响应
async fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "404").into_response()
}
