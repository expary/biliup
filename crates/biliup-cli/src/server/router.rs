use crate::server::api::bilibili_endpoints::{
    archive_pre_endpoint, get_myinfo_endpoint, get_proxy_endpoint,
};
use crate::server::api::endpoints::{
    add_upload_streamer_endpoint, add_user_endpoint, delete_template_endpoint, delete_user_endpoint,
    get_configuration, get_metrics, get_qrcode,
    get_upload_streamer_endpoint, get_upload_streamers_endpoint, get_users_endpoint, get_videos,
    login_by_qrcode, post_uploads, put_configuration,
};
use crate::server::api::youtube_endpoints::{
    delete_youtube_item_endpoint, delete_youtube_job_endpoint, get_youtube_active_endpoint,
    get_youtube_global_items_endpoint, get_youtube_item_logs_endpoint,
    get_youtube_job_items_endpoint, get_youtube_job_logs_endpoint, get_youtube_jobs_endpoint,
    get_youtube_manager_health_endpoint, pause_youtube_item_endpoint, pause_youtube_job_endpoint,
    pause_youtube_queue_endpoint, post_youtube_jobs_endpoint, put_youtube_jobs_endpoint,
    retry_failed_youtube_job_endpoint, retry_failed_youtube_queue_endpoint, retry_youtube_item_endpoint,
    run_youtube_item_endpoint, run_youtube_job_endpoint, run_youtube_queue_endpoint,
    sync_youtube_job_endpoint,
};
use crate::server::infrastructure::service_register::ServiceRegister;
use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post, put};
use tower::ServiceExt;
use tower_http::services::ServeFile;
/// 创建应用程序路由
pub fn router(service_register: ServiceRegister) -> Router<()> {
    Router::new()
        // 配置管理路由
        .route(
            "/v1/configuration",
            get(get_configuration).put(put_configuration), // 获取/更新配置
        )
        // 上传模板管理路由
        .route("/v1/upload/streamers", get(get_upload_streamers_endpoint)) // 获取上传模板列表
        .route(
            "/v1/upload/streamers/{id}",
            delete(delete_template_endpoint) // 删除上传模板
                .get(get_upload_streamer_endpoint), // 获取单个上传模板
        )
        .route("/v1/upload/streamers", post(add_upload_streamer_endpoint)) // 添加上传模板
        // 用户管理路由
        .route("/v1/users", get(get_users_endpoint).post(add_user_endpoint)) // 获取用户列表/添加用户
        .route("/v1/users/{id}", delete(delete_user_endpoint)) // 删除用户
        // B站API代理路由
        .route("/bili/archive/pre", get(archive_pre_endpoint)) // 投稿预处理
        .route("/bili/space/myinfo", get(get_myinfo_endpoint)) // 获取用户信息
        .route("/bili/proxy", get(get_proxy_endpoint)) // 代理请求
        // 认证相关路由
        .route("/v1/get_qrcode", get(get_qrcode)) // 获取二维码
        .route("/v1/login_by_qrcode", post(login_by_qrcode)) // 二维码登录
        // 视频文件管理路由
        .route("/v1/videos", get(get_videos)) // 获取视频列表
        .route("/v1/metrics", get(get_metrics))
        .route("/v1/uploads", post(post_uploads))
        .route(
            "/v1/youtube/jobs",
            get(get_youtube_jobs_endpoint).post(post_youtube_jobs_endpoint),
        )
        .route(
            "/v1/youtube/jobs/{id}",
            put(put_youtube_jobs_endpoint).delete(delete_youtube_job_endpoint),
        )
        .route("/v1/youtube/jobs/{id}/run", post(run_youtube_job_endpoint))
        .route("/v1/youtube/jobs/{id}/sync_now", post(sync_youtube_job_endpoint))
        .route(
            "/v1/youtube/jobs/{id}/pause",
            post(pause_youtube_job_endpoint),
        )
        .route(
            "/v1/youtube/jobs/{id}/retry_failed",
            post(retry_failed_youtube_job_endpoint),
        )
        .route(
            "/v1/youtube/jobs/{id}/items",
            get(get_youtube_job_items_endpoint),
        )
        .route("/v1/youtube/items", get(get_youtube_global_items_endpoint))
        .route("/v1/youtube/queue/run", post(run_youtube_queue_endpoint))
        .route("/v1/youtube/queue/pause", post(pause_youtube_queue_endpoint))
        .route(
            "/v1/youtube/queue/retry_failed",
            post(retry_failed_youtube_queue_endpoint),
        )
        .route("/v1/youtube/items/{id}/run", post(run_youtube_item_endpoint))
        .route("/v1/youtube/items/{id}/pause", post(pause_youtube_item_endpoint))
        .route(
            "/v1/youtube/items/{id}",
            delete(delete_youtube_item_endpoint),
        )
        .route("/v1/youtube/items/{id}/retry", post(retry_youtube_item_endpoint))
        .route(
            "/v1/youtube/items/{id}/logs",
            get(get_youtube_item_logs_endpoint),
        )
        .route(
            "/v1/youtube/jobs/{id}/logs",
            get(get_youtube_job_logs_endpoint),
        )
        .route(
            "/v1/youtube/manager/health",
            get(get_youtube_manager_health_endpoint),
        )
        .route("/v1/youtube/active", get(get_youtube_active_endpoint))
        .route_service("/static/{path}", get(using_serve_file_from_a_route))
        .with_state(service_register) // 注入服务注册器状态
}

async fn using_serve_file_from_a_route(
    axum::extract::Path(path): axum::extract::Path<String>,
    request: Request<Body>,
) -> impl IntoResponse {
    let serve_file = ServeFile::new(path);
    serve_file.oneshot(request).await
}
