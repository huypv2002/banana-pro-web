export async function onRequest(context) {
  const VPS = "http://148.163.121.139:8088";
  const request = context.request;
  const url = new URL(request.url);

  // Chỉ proxy các path /api/*
  const path = url.pathname.replace(/^\/api/, "") || "/";
  const target = VPS + path + url.search;

  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,DELETE,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
      },
    });
  }

  const resp = await fetch(target, {
    method: request.method,
    headers: { "Content-Type": "application/json" },
    body: request.method !== "GET" ? request.body : undefined,
  });

  const headers = new Headers(resp.headers);
  headers.set("Access-Control-Allow-Origin", "*");

  return new Response(resp.body, { status: resp.status, headers });
}
