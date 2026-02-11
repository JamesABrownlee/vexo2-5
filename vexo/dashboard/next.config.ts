import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: 'standalone',
  // External packages that should not be bundled
  serverExternalPackages: ['better-sqlite3'],

  // Empty turbopack config to silence warning (use defaults)
  turbopack: {},

  // Enable experimental features for native modules
  experimental: {
    serverActions: {
      bodySizeLimit: '2mb',
    },
  },

  async rewrites() {
    return [
      {
        source: '/api/bot/:path*',
        destination: `${process.env.BOT_API_URL || 'http://musicbot:8080'}/api/:path*`,
      },
    ];
  },
};

export default nextConfig;
