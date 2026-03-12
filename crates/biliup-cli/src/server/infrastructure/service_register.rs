use crate::LogHandle;
use crate::server::config::Config;
use crate::server::infrastructure::connection_pool::ConnectionPool;
use crate::server::youtube::manager::YouTubeJobManager;
use axum::extract::FromRef;
use biliup::client::StatelessClient;
use error_stack::Report;
use error_stack::fmt::ColorMode;
use std::sync::{Arc, RwLock};
use tracing::info;

/// 服务注册器
/// 负责管理应用程序中的各种服务实例，包括数据库连接池、工作器、下载管理器等
#[derive(FromRef, Clone)]
pub struct ServiceRegister {
    /// 数据库连接池
    pub pool: ConnectionPool,
    /// 全局配置
    pub config: Arc<RwLock<Config>>,
    /// HTTP客户端
    pub client: StatelessClient,
    /// YouTube搬运任务管理器
    pub youtube_manager: Arc<YouTubeJobManager>,

    pub log_handle: LogHandle,
}

/// 简单的服务容器，负责管理API端点通过axum扩展获取的各种服务
impl ServiceRegister {
    /// 创建新的服务注册器实例
    ///
    /// # 参数
    /// * `pool` - 数据库连接池
    /// * `config` - 全局配置
    /// * `actor_handle` - Actor处理器
    /// * `download_manager` - 下载管理器列表
    pub fn new(
        pool: ConnectionPool,
        config: Arc<RwLock<Config>>,
        log_handle: LogHandle,
    ) -> Self {
        Report::set_color_mode(ColorMode::None);
        info!("initializing utility services...");
        // 创建默认的HTTP客户端
        let client = StatelessClient::default();

        info!("utility services initialized, building feature services...");

        let youtube_manager = YouTubeJobManager::new(pool.clone(), config.clone());
        youtube_manager.clone().start();

        info!("feature services successfully initialized!");
        ServiceRegister {
            pool,
            config: config.clone(),
            client,
            youtube_manager,
            log_handle,
        }
    }

    pub async fn cleanup(&self) {
        let _ = &self.client;
    }
}

// impl FromRef<ServiceRegister> for ConnectionPool {
//     fn from_ref(app_state: &ServiceRegister) -> ConnectionPool {
//         app_state.pool.clone()
//     }
// }
