const API_BASE = (process.env.NEXT_PUBLIC_API_BASE || "/api/v1").replace(/\/+$/, "");

function apiUrl(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${API_BASE}${normalizedPath}`;
}

function responseSnippet(raw: string, max = 160): string {
  return raw.replace(/\s+/g, " ").trim().slice(0, max);
}

async function parseJsonResponse<T>(method: string, path: string, response: Response): Promise<T> {
  const contentType = response.headers.get("content-type") ?? "";
  const raw = await response.text();

  if (!response.ok) {
    const suffix = raw ? ` ${responseSnippet(raw)}` : "";
    throw new Error(`${method} ${path} failed: ${response.status}${suffix}`);
  }

  if (!contentType.includes("application/json")) {
    throw new Error(
      `${method} ${path} expected JSON but got ${contentType || "unknown content-type"}: ${responseSnippet(raw)}`,
    );
  }

  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new Error(`${method} ${path} returned invalid JSON: ${responseSnippet(raw)}`);
  }
}

const API_SHARED_HEADERS = {
  Accept: "application/json",
  "ngrok-skip-browser-warning": "true",
};

export async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(apiUrl(path), {
    cache: "no-store",
    headers: API_SHARED_HEADERS,
  });
  return parseJsonResponse<T>("GET", path, response);
}

export async function apiPost<T>(path: string, payload: unknown): Promise<T> {
  const response = await fetch(apiUrl(path), {
    method: "POST",
    headers: {
      ...API_SHARED_HEADERS,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return parseJsonResponse<T>("POST", path, response);
}
