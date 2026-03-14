import { Routes, Route } from "react-router-dom";
import { Layout } from "./layout/Layout";
import { PortfolioProvider } from "./context/PortfolioContext";
import Dashboard from "./pages/Dashboard";
import Portfolio from "./pages/Portfolio";
import Backtests from "./pages/Backtests";
import Regime from "./pages/Regime";
import Entities from "./pages/Entities";
import LogsReports from "./pages/LogsReports";
import Intelligence from "./pages/Intelligence";
import NationProfile from "./pages/NationProfile";
import GeoRisk from "./pages/GeoRisk";
import Settings from "./pages/Settings";
import Chat from "./pages/Chat";
import PersonProfile from "./pages/PersonProfile";
import ResourceProfile from "./pages/ResourceProfile";
import TradeProfile from "./pages/TradeProfile";

export default function App() {
  return (
    <PortfolioProvider>
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="portfolio" element={<Portfolio />} />
        <Route path="backtests" element={<Backtests />} />
        <Route path="regime" element={<Regime />} />
        <Route path="entities" element={<Entities />} />
        <Route path="logs" element={<LogsReports />} />
        <Route path="nation" element={<NationProfile />} />
        <Route path="person" element={<PersonProfile />} />
        <Route path="geo" element={<GeoRisk />} />
        <Route path="resource" element={<ResourceProfile />} />
        <Route path="trade" element={<TradeProfile />} />
        <Route path="intelligence" element={<Intelligence />} />
        <Route path="settings" element={<Settings />} />
        <Route path="chat" element={<Chat />} />
      </Route>
    </Routes>
    </PortfolioProvider>
  );
}
