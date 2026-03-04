pub fn parse_job_log_message(raw: &str) -> (String, Option<String>, String) {
    let raw_trimmed = raw.trim();
    if raw_trimmed.is_empty() {
        return ("日志".to_string(), None, "".to_string());
    }

    let mut stage = "日志".to_string();
    let mut rest = raw_trimmed;
    if rest.starts_with('[') {
        if let Some(end) = rest.find(']') {
            let candidate = rest[1..end].trim();
            if !candidate.is_empty() {
                stage = candidate.to_string();
            }
            rest = rest[end + 1..].trim();
        }
    }

    let mut video_id: Option<String> = None;
    let mut message = rest.trim();
    if let Some(after_vid) = message.strip_prefix("vid=") {
        let after_vid = after_vid.trim_start();
        let mut it = after_vid.splitn(2, char::is_whitespace);
        if let Some(vid) = it.next() {
            if !vid.trim().is_empty() {
                video_id = Some(vid.trim().to_string());
            }
        }
        message = it.next().unwrap_or("").trim();
    }

    let message = if message.is_empty() {
        raw_trimmed.to_string()
    } else {
        message.to_string()
    };
    (stage, video_id, message)
}

