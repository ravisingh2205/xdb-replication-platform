import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// In dev, /api is proxied to the API Gateway. In the container, nginx does the same.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { '/api': { target: process.env.VITE_GATEWAY_URL ?? 'http://localhost:8080', changeOrigin: true } },
  },
});
