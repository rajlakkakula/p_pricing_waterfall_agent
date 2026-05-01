import { useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { postChat } from "../api";

// ── Markdown renderer — turns Claude's markdown into styled HTML ───────────────

const MD_COMPONENTS = {
  // Tables — clean, bordered, readable
  table: ({ children }) => (
    <div className="overflow-x-auto my-3 rounded-lg border border-gray-200 shadow-sm">
      <table className="w-full text-sm border-collapse">{children}</table>
    </div>
  ),
  thead: ({ children }) => (
    <thead className="bg-gray-50 border-b border-gray-200">{children}</thead>
  ),
  tbody: ({ children }) => <tbody className="divide-y divide-gray-100">{children}</tbody>,
  tr: ({ children }) => (
    <tr className="hover:bg-blue-50 transition-colors">{children}</tr>
  ),
  th: ({ children }) => (
    <th className="px-4 py-2.5 text-left text-xs font-semibold text-gray-500 uppercase tracking-wide whitespace-nowrap">
      {children}
    </th>
  ),
  td: ({ children }) => (
    <td className="px-4 py-2.5 text-sm text-gray-800 whitespace-nowrap">{children}</td>
  ),

  // Paragraphs
  p: ({ children }) => (
    <p className="text-sm text-gray-700 leading-relaxed mb-2 last:mb-0">{children}</p>
  ),

  // Inline code
  code: ({ inline, children }) =>
    inline ? (
      <code className="bg-gray-100 text-blue-700 px-1 py-0.5 rounded text-xs font-mono">
        {children}
      </code>
    ) : (
      <code className="block bg-gray-900 text-gray-100 p-3 rounded-lg text-xs font-mono overflow-x-auto my-2">
        {children}
      </code>
    ),

  // Bold / strong
  strong: ({ children }) => (
    <strong className="font-semibold text-gray-900">{children}</strong>
  ),

  // Lists
  ul: ({ children }) => (
    <ul className="list-disc list-inside text-sm text-gray-700 space-y-0.5 mb-2 pl-1">
      {children}
    </ul>
  ),
  ol: ({ children }) => (
    <ol className="list-decimal list-inside text-sm text-gray-700 space-y-0.5 mb-2 pl-1">
      {children}
    </ol>
  ),
  li: ({ children }) => <li className="leading-relaxed">{children}</li>,

  // Headings
  h3: ({ children }) => (
    <h3 className="text-sm font-semibold text-gray-800 mt-3 mb-1">{children}</h3>
  ),
  h4: ({ children }) => (
    <h4 className="text-xs font-semibold text-gray-600 uppercase tracking-wide mt-2 mb-1">
      {children}
    </h4>
  ),
};

// ── SQL query panel ────────────────────────────────────────────────────────────

function SqlPanel({ queries }) {
  const [open, setOpen] = useState(false);
  if (!queries?.length) return null;
  const label = queries.length === 1 ? "1 SQL query" : `${queries.length} SQL queries`;

  return (
    <div className="mt-3 border-t border-gray-100 pt-2">
      <button
        onClick={() => setOpen((s) => !s)}
        className="flex items-center gap-1.5 text-xs text-gray-400 hover:text-blue-600 transition-colors font-medium"
      >
        <svg
          className={`w-3 h-3 transition-transform ${open ? "rotate-90" : ""}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={2}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
        </svg>
        {label}
      </button>

      {open && (
        <div className="mt-2 space-y-2">
          {queries.map((sql, i) => (
            <pre
              key={i}
              className="bg-gray-900 text-green-300 text-xs font-mono p-3 rounded-lg overflow-x-auto leading-relaxed"
            >
              {sql.trim()}
            </pre>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Message bubbles ────────────────────────────────────────────────────────────

function UserBubble({ text }) {
  return (
    <div className="flex justify-end">
      <div className="max-w-[85%] bg-blue-600 text-white px-4 py-2.5 rounded-2xl rounded-br-sm text-sm shadow-sm">
        {text}
      </div>
    </div>
  );
}

function AgentBubble({ msg }) {
  const { data, error, elapsed_ms } = msg;

  if (error) {
    return (
      <div className="flex justify-start">
        <div className="max-w-[90%] bg-red-50 border border-red-200 px-4 py-3 rounded-2xl rounded-bl-sm text-sm text-red-700">
          {error}
        </div>
      </div>
    );
  }

  return (
    <div className="flex justify-start">
      <div className="max-w-[94%] bg-white border border-gray-200 px-4 py-3 rounded-2xl rounded-bl-sm shadow-sm">
        {/* Header */}
        <div className="flex items-center gap-2 mb-2">
          <span className="text-xs font-semibold px-2 py-0.5 rounded-full bg-blue-100 text-blue-700">
            SQL
          </span>
          {elapsed_ms != null && (
            <span className="text-xs text-gray-400">{(elapsed_ms / 1000).toFixed(1)}s</span>
          )}
        </div>

        {/* Markdown answer — tables render as proper HTML */}
        {data?.answer ? (
          <ReactMarkdown remarkPlugins={[remarkGfm]} components={MD_COMPONENTS}>
            {data.answer}
          </ReactMarkdown>
        ) : (
          <p className="text-sm text-gray-400">No results returned.</p>
        )}

        {/* Collapsible SQL panel */}
        <SqlPanel queries={data?.sql_queries} />
      </div>
    </div>
  );
}

function ThinkingBubble() {
  return (
    <div className="flex justify-start">
      <div className="bg-gray-100 border border-gray-200 px-4 py-3 rounded-2xl rounded-bl-sm">
        <div className="flex gap-1 items-center h-4">
          {[0, 1, 2].map((i) => (
            <span
              key={i}
              className="w-1.5 h-1.5 bg-blue-400 rounded-full animate-bounce"
              style={{ animationDelay: `${i * 0.15}s` }}
            />
          ))}
          <span className="text-xs text-gray-400 ml-2">Querying…</span>
        </div>
      </div>
    </div>
  );
}

// ── Suggestion chips ───────────────────────────────────────────────────────────

const SUGGESTIONS = [
  "Which are the top 5 materials with the highest bonuses?",
  "What is the margin % for each country?",
  "Which customers in EMEA have margin below 15%?",
  "Compare deduction rates across PSOs for 2025",
  "Which corporate groups are in Tier 4 or Tier 5?",
  "How did margin change from 2024 to 2025?",
];

// ── Main component ─────────────────────────────────────────────────────────────

export default function AgentChat({ onDashboardUpdate }) {
  const [messages, setMessages] = useState([]);
  const [input, setInput]       = useState("");
  const [loading, setLoading]   = useState(false);
  const bottomRef               = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const send = async (query) => {
    if (!query.trim() || loading) return;
    setInput("");
    setMessages((m) => [...m, { role: "user", text: query }]);
    setLoading(true);

    try {
      const data = await postChat(query);
      setMessages((m) => [
        ...m,
        {
          role: "agent",
          data,
          error: data.status === "error" ? (data.error ?? "An error occurred.") : null,
          elapsed_ms: data.elapsed_ms,
        },
      ]);
    } catch (err) {
      setMessages((m) => [...m, { role: "agent", error: err.message }]);
    } finally {
      setLoading(false);
    }
  };

  const handleKey = (e) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(input); }
  };

  return (
    <div className="flex flex-col h-full bg-white">
      {/* Header */}
      <div className="px-4 py-3 border-b border-gray-200 bg-white">
        <div className="flex items-center gap-2">
          <div className="w-6 h-6 rounded-full bg-orange-500 flex items-center justify-center flex-shrink-0">
            <span className="text-white text-xs font-bold">AI</span>
          </div>
          <div>
            <h2 className="font-semibold text-gray-900 text-sm">Pricing Agent</h2>
            <p className="text-xs text-gray-400">Ask anything — answers come from live SQL</p>
          </div>
        </div>
      </div>

      {/* Message list */}
      <div className="flex-1 overflow-y-auto p-4 space-y-3 scrollbar-thin bg-gray-50">
        {messages.length === 0 && (
          <div className="flex flex-col gap-2 mt-4">
            <p className="text-sm text-gray-400 text-center mb-2">Try asking…</p>
            {SUGGESTIONS.map((s) => (
              <button
                key={s}
                onClick={() => send(s)}
                className="text-left text-sm text-gray-700 bg-white hover:bg-blue-50
                           border border-gray-200 hover:border-blue-300 rounded-xl px-4 py-2.5
                           transition-colors shadow-sm"
              >
                {s}
              </button>
            ))}
          </div>
        )}

        {messages.map((msg, i) =>
          msg.role === "user"
            ? <UserBubble key={i} text={msg.text} />
            : <AgentBubble key={i} msg={msg} />
        )}

        {loading && <ThinkingBubble />}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="p-3 border-t border-gray-200 bg-white">
        <div className="flex gap-2">
          <textarea
            rows={1}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKey}
            placeholder="Ask about margins, bonuses, customers, trends…"
            disabled={loading}
            className="flex-1 bg-white border border-gray-300 text-gray-900 text-sm rounded-xl
                       px-4 py-2.5 resize-none focus:outline-none focus:ring-2 focus:ring-blue-500
                       focus:border-blue-500 disabled:opacity-50 placeholder-gray-400 transition-colors"
          />
          <button
            onClick={() => send(input)}
            disabled={loading || !input.trim()}
            className="px-4 bg-orange-500 hover:bg-orange-600 active:bg-orange-700
                       disabled:opacity-40 disabled:cursor-not-allowed
                       text-white rounded-xl font-semibold text-sm transition-colors shadow-sm"
          >
            Send
          </button>
        </div>
        <p className="text-xs text-gray-300 mt-1.5 pl-1">Enter to send · Shift+Enter for newline</p>
      </div>
    </div>
  );
}
