/** @type {import('next').NextConfig} */
const API_PROXY_TARGET = (process.env.API_PROXY_TARGET || "https://b599-171-76-80-80.ngrok-free.app/api/v1").replace(
  /\/+$/,
  "",
);

const nextConfig = {
  reactStrictMode: true,
  async rewrites() {
    return [
      {
        source: "/api/v1/:path*",
        destination: `${API_PROXY_TARGET}/:path*`,
      },
    ];
  },
};

export default nextConfig;
