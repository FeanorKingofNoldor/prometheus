import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    proxy: {
      // Info-layer routes → Apathis API (:8100)
      "/api/nation": {
        target: "http://localhost:8100",
        changeOrigin: true,
      },
      "/api/intel": {
        target: "http://localhost:8100",
        changeOrigin: true,
      },
      "/api/entities": {
        target: "http://localhost:8100",
        changeOrigin: true,
      },
      "/api/chat": {
        target: "http://localhost:8100",
        changeOrigin: true,
      },
      "/api/auth": {
        target: "http://localhost:8100",
        changeOrigin: true,
      },
      "/api/billing": {
        target: "http://localhost:8100",
        changeOrigin: true,
      },
      "/api/status/regime": {
        target: "http://localhost:8100",
        changeOrigin: true,
      },
      "/api/status/stability": {
        target: "http://localhost:8100",
        changeOrigin: true,
      },
      "/api/status/fragility": {
        target: "http://localhost:8100",
        changeOrigin: true,
      },
      "/api/status/sector_health": {
        target: "http://localhost:8100",
        changeOrigin: true,
      },
      "/api/status/docs": {
        target: "http://localhost:8100",
        changeOrigin: true,
      },
      // All other /api/* routes → Prometheus trading API (:8000)
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});
