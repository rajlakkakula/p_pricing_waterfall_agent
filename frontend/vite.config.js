import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  // Sub-path required when hosted at https://<user>.github.io/<repo>/
  base: process.env.VITE_BASE_PATH ?? "/",
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
});
