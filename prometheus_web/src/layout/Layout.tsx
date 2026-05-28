import { Outlet } from "react-router-dom";
import { Sidebar } from "./Sidebar";
import { TopBar } from "./TopBar";
import IrisOverlay from "../components/IrisOverlay";

export function Layout() {
  return (
    <div className="flex h-screen overflow-hidden bg-surface">
      <Sidebar />
      <div className="flex flex-1 flex-col overflow-hidden">
        <TopBar />
        <main className="flex-1 overflow-y-auto p-4">
          <Outlet />
        </main>
      </div>
      {/* Iris drawer + brain FAB — overlays the whole app, persists state
          across route changes via IrisProvider in App.tsx. */}
      <IrisOverlay />
    </div>
  );
}
