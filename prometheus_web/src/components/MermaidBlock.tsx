import { useEffect, useRef, useState, useCallback } from "react";
import mermaid from "mermaid";

let mermaidInitialized = false;

function initMermaid() {
  if (mermaidInitialized) return;
  mermaid.initialize({
    startOnLoad: false,
    theme: "dark",
    themeVariables: {
      darkMode: true,
      background: "#18181b",
      primaryColor: "#3b82f6",
      primaryTextColor: "#e4e4e7",
      primaryBorderColor: "#3f3f46",
      lineColor: "#71717a",
      secondaryColor: "#27272a",
      tertiaryColor: "#1e1e22",
      fontFamily: "ui-monospace, monospace",
      fontSize: "16px",
      nodeTextSize: "16px",
    },
    flowchart: { curve: "basis", padding: 16, nodeSpacing: 30, rankSpacing: 40 },
    securityLevel: "loose",
  });
  mermaidInitialized = true;
}

let idCounter = 0;

const ZOOM_PRESETS = [0.5, 1, 1.5, 2, 3, 5, 8] as const;
const MIN_ZOOM = 0.2;
const MAX_ZOOM = 10;

/** Build a standalone HTML page string for the pop-out window. */
function buildPopoutHtml(svgMarkup: string): string {
  return `<!DOCTYPE html>
<html><head><title>Prometheus — Diagram</title>
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  html,body{background:#18181b;color:#e4e4e7;font-family:ui-monospace,monospace;overflow:auto;height:100%}
  #toolbar{position:fixed;top:0;left:0;right:0;z-index:10;display:flex;align-items:center;gap:6px;padding:8px 16px;background:#18181bee;backdrop-filter:blur(8px);border-bottom:1px solid #27272a}
  #toolbar button{border:none;background:#27272a;color:#a1a1aa;padding:3px 8px;border-radius:4px;font-size:11px;cursor:pointer}
  #toolbar button:hover{color:#e4e4e7;background:#3f3f46}
  #toolbar button.active{background:rgba(59,130,246,0.2);color:#3b82f6}
  #toolbar span{font-size:10px;color:#71717a}
  #canvas{padding:60px 24px 24px;cursor:grab}
  #canvas.dragging{cursor:grabbing}
  #diagram{transform-origin:top left;display:inline-block}
</style></head><body>
<div id="toolbar">
  <span>Zoom:</span>
  <button onclick="sz(0.5)">50%</button>
  <button onclick="sz(1)" class="active">1x</button>
  <button onclick="sz(1.5)">1.5x</button>
  <button onclick="sz(2)">2x</button>
  <button onclick="sz(3)">3x</button>
  <button onclick="sz(5)">5x</button>
  <button onclick="sz(8)">8x</button>
  <button onclick="zi()">+</button>
  <button onclick="zo()">\u2212</button>
  <span id="zlbl">100%</span>
  <span style="margin-left:auto">Ctrl+scroll to zoom \u00b7 drag to pan</span>
</div>
<div id="canvas"><div id="diagram">${svgMarkup}</div></div>
<script>
let s=1;
const d=document.getElementById('diagram'),c=document.getElementById('canvas'),l=document.getElementById('zlbl');
function up(){d.style.transform='scale('+s+')';l.textContent=Math.round(s*100)+'%';document.querySelectorAll('#toolbar button').forEach(b=>{const v=parseFloat(b.textContent);if(!isNaN(v)&&b.textContent.includes('x')){b.className=Math.abs(s-v)<0.05?'active':''}else if(b.textContent.includes('%')){b.className=Math.abs(s-parseFloat(b.textContent)/100)<0.05?'active':''}})}
function sz(v){s=v;up()}
function zi(){s=Math.min(10,s+0.5);up()}
function zo(){s=Math.max(0.2,s-0.5);up()}
c.addEventListener('wheel',e=>{if(e.ctrlKey||e.metaKey){e.preventDefault();s=Math.max(0.2,Math.min(10,s*(1-e.deltaY*0.003)));up()}},{passive:false});
let drag=false,sx=0,sy=0,slx=0,sly=0;
c.addEventListener('mousedown',e=>{if(e.button===0){drag=true;sx=e.clientX;sy=e.clientY;slx=window.scrollX;sly=window.scrollY;c.classList.add('dragging');e.preventDefault()}});
window.addEventListener('mousemove',e=>{if(!drag)return;window.scrollTo(slx-(e.clientX-sx),sly-(e.clientY-sy))});
window.addEventListener('mouseup',()=>{drag=false;c.classList.remove('dragging')});
</script></body></html>`;
}

export function MermaidBlock({ code }: { code: string }) {
  const ref = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [svg, setSvg] = useState("");
  const [error, setError] = useState("");
  const [scale, setScale] = useState(1);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [isDragging, setIsDragging] = useState(false);
  const dragStart = useRef({ x: 0, y: 0, scrollX: 0, scrollY: 0 });

  useEffect(() => {
    initMermaid();
    const id = `mermaid-${++idCounter}`;
    let cancelled = false;

    mermaid
      .render(id, code.trim())
      .then(({ svg: rendered }) => {
        if (!cancelled) setSvg(rendered);
      })
      .catch((err) => {
        if (!cancelled) setError(String(err));
      });

    return () => {
      cancelled = true;
    };
  }, [code]);

  // Escape key exits fullscreen
  useEffect(() => {
    if (!isFullscreen) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") setIsFullscreen(false);
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [isFullscreen]);

  const handleWheel = useCallback((e: React.WheelEvent) => {
    if (e.ctrlKey || e.metaKey) {
      e.preventDefault();
      setScale((s) => Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, s * (1 - e.deltaY * 0.003))));
    }
  }, []);

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    if (e.button !== 0) return;
    const ct = containerRef.current;
    if (!ct) return;
    setIsDragging(true);
    dragStart.current = { x: e.clientX, y: e.clientY, scrollX: ct.scrollLeft, scrollY: ct.scrollTop };
  }, []);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (!isDragging) return;
    const ct = containerRef.current;
    if (!ct) return;
    ct.scrollLeft = dragStart.current.scrollX - (e.clientX - dragStart.current.x);
    ct.scrollTop = dragStart.current.scrollY - (e.clientY - dragStart.current.y);
  }, [isDragging]);

  const handleMouseUp = useCallback(() => setIsDragging(false), []);

  const openInNewWindow = useCallback(() => {
    if (!svg) return;
    const win = window.open("", "_blank", "width=1400,height=900");
    if (!win) return;
    win.document.write(buildPopoutHtml(svg));
    win.document.close();
  }, [svg]);

  if (error) {
    return (
      <div className="rounded border border-red-800 bg-red-950/30 p-3 text-xs text-red-400">
        Mermaid render error: {error}
      </div>
    );
  }

  const toolbar = (
    <div className="flex items-center gap-1 mb-2 flex-wrap">
      <span className="text-[10px] text-muted mr-1">Zoom:</span>
      {ZOOM_PRESETS.map((z) => (
        <button
          key={z}
          onClick={() => setScale(z)}
          className={`rounded px-1.5 py-0.5 text-[10px] transition-colors ${
            Math.abs(scale - z) < 0.05
              ? "bg-accent/20 text-accent font-medium"
              : "bg-zinc-800 text-zinc-400 hover:text-zinc-200"
          }`}
        >
          {z < 1 ? `${z * 100}%` : `${z}x`}
        </button>
      ))}
      <button
        onClick={() => setScale((s) => Math.min(MAX_ZOOM, s + 0.5))}
        className="rounded bg-zinc-800 px-1.5 py-0.5 text-[10px] text-zinc-400 hover:text-zinc-200"
      >
        +
      </button>
      <button
        onClick={() => setScale((s) => Math.max(MIN_ZOOM, s - 0.5))}
        className="rounded bg-zinc-800 px-1.5 py-0.5 text-[10px] text-zinc-400 hover:text-zinc-200"
      >
        −
      </button>

      <div className="ml-auto flex items-center gap-1">
        <span className="text-[9px] text-muted mr-2">
          {Math.round(scale * 100)}%
        </span>
        <button
          onClick={() => setIsFullscreen((f) => !f)}
          className="rounded bg-zinc-800 px-2 py-0.5 text-[10px] text-zinc-400 hover:text-zinc-200"
          title={isFullscreen ? "Exit fullscreen (Esc)" : "Fullscreen"}
        >
          {isFullscreen ? "✖ Exit" : "⛶ Fullscreen"}
        </button>
        <button
          onClick={openInNewWindow}
          className="rounded bg-zinc-800 px-2 py-0.5 text-[10px] text-zinc-400 hover:text-zinc-200"
          title="Open in new window"
        >
          ↗ New Window
        </button>
      </div>
    </div>
  );

  const diagramView = (
    <div
      ref={containerRef}
      className={`overflow-auto rounded-lg border border-border-dim bg-[#18181b] p-4 ${
        isFullscreen ? "" : "max-h-[80vh]"
      } ${
        isDragging ? "cursor-grabbing" : "cursor-grab"
      }`}
      style={isFullscreen ? { height: "calc(100vh - 48px)" } : undefined}
      onWheel={handleWheel}
      onMouseDown={handleMouseDown}
      onMouseMove={handleMouseMove}
      onMouseUp={handleMouseUp}
      onMouseLeave={handleMouseUp}
    >
      <div
        ref={ref}
        style={{ transform: `scale(${scale})`, transformOrigin: "top left", minWidth: "max-content" }}
        dangerouslySetInnerHTML={{ __html: svg }}
      />
    </div>
  );

  // Fullscreen overlay
  if (isFullscreen) {
    return (
      <div className="fixed inset-0 z-50 bg-[#18181b] flex flex-col">
        <div className="px-4 pt-2">
          {toolbar}
        </div>
        <div className="flex-1 min-h-0 px-4 pb-4">
          {diagramView}
        </div>
      </div>
    );
  }

  return (
    <div className="relative my-4">
      {toolbar}
      {diagramView}
    </div>
  );
}
