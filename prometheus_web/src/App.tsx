import { Routes, Route } from "react-router-dom";
import { Layout } from "./layout/Layout";
import { PortfolioProvider } from "./context/PortfolioContext";
import Dashboard from "./pages/Dashboard";
import Portfolio from "./pages/Portfolio";
import Backtests from "./pages/Backtests";
import Execution from "./pages/Execution";
import Options from "./pages/Options";
import LogsReports from "./pages/LogsReports";
import Settings from "./pages/Settings";
import Chat from "./pages/Chat";

export default function App() {
  return (
    <PortfolioProvider>
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="portfolio" element={<Portfolio />} />
        <Route path="execution" element={<Execution />} />
        <Route path="backtests" element={<Backtests />} />
        <Route path="options" element={<Options />} />
        <Route path="logs" element={<LogsReports />} />
        <Route path="settings" element={<Settings />} />
        <Route path="kronos" element={<Chat />} />
      </Route>
    </Routes>
    </PortfolioProvider>
  );
}
