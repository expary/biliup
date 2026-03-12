// Fetcher implementation. // The extra argument will be passed via the `arg` property of the 2nd parameter.// In the example below, `arg` will be `'my_token'`
export const API_BASE = process.env.NEXT_PUBLIC_API_SERVER ?? '';
export async function sendRequest<T>(url: string, { arg }: { arg: T }) {
	const res = await fetch(API_BASE + url, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(arg),
	});
	await handleResponse(res);
	return res.json();
}

export const fetcher = async (input: RequestInfo | URL, init?: RequestInit) => {
	const res = await fetch(API_BASE + input, init);
	await handleResponse(res);
	return res.json();
};

export const proxy = async (input: RequestInfo | URL, init?: RequestInit) => {
	const res = await fetch(API_BASE + input, init);
	await handleResponse(res);
	return res;
};

export async function requestDelete<T>(url: string, { arg }: { arg: T }) {
	const res = await fetch(`${API_BASE}${url}/${arg}`, { method: 'DELETE' });
	await handleResponse(res);
	return res;
}

export async function put<T>(url: string, { arg }: { arg: T }) {
	const res = await fetch(`${API_BASE}${url}`, {
		method: 'PUT',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(arg),
	});
	await handleResponse(res);
	return res;
}

async function handleResponse(res: Response) {
	// 如果未登录，统一跳转
	if (res.status === 401) {
		// 可选：清理本地状态/缓存
		// localStorage.removeItem('token') 等

		// 跳转登录（带回跳）
		const returnTo = encodeURIComponent(window.location.pathname + window.location.search);
		window.location.href = `/login?next=${returnTo}`;
		// 抛错让 SWR 知道失败（别返回 json）
		throw new Error('Unauthorized');
	}

	if (!res.ok) {
		// 尽量返回服务端错误信息
		const text = await res.text().catch(() => '');
		if (text) {
			try {
				const parsed = JSON.parse(text);
				const message = parsed?.message;
				if (typeof message === 'string' && message.trim()) {
					throw new Error(message);
				}
			} catch {
				// ignore json parse error
			}
		}
		throw new Error(text || `HTTP ${res.status}`);
	}
	return res;
}

type Credit = {
	username: string;
	uid: number;
};

export interface StudioEntity {
	id: number;
	template_name: string;
	user_cookie: string;
	copyright: number;
	copyright_source: string;
	tid: number;
	cover_path: string;
	title: string;
	description: string;
	dynamic: string;
	tags: string[];
	dtime: number;
	// interactive: number;
	mission_id?: number;
	dolby: number;
	hires: number;
	no_reprint: number;
	is_only_self: number;
	up_selection_reply: number;
	up_close_reply: number;
	up_close_danmu: number;
	charging_pay: number;
	credits: Credit[];
	uploader: string;
	extra_fields?: string;
	youtube_title_strategy?: string;
	youtube_title_strategy_prompt?: string;
	youtube_mark_source_link?: number | boolean;
	youtube_mark_source_channel?: number | boolean;
}

export interface BiliType {
	id: number;
	children: BiliType[];
	name: string;
	desc: string;
}

export interface User {
	id: number;
	name: string;
	value: string;
	platform: string;
}

export interface FileList {
	key: number;
	name: string;
	updateTime: number;
	size: number;
}
