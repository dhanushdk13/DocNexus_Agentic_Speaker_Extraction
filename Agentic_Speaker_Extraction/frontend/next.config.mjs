/** @type {import('next').NextConfig} */
const API_PROXY_TARGET = "https://6c31-2a09-bac6-d9d3-eaa-00-176-a2.ngrok-free.app/api/v1".replace(/\/+$/, "");

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
