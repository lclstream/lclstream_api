import { StrictMode } from "react"
import ReactDOM from "react-dom/client"

import { TransfersPanel } from "./TransfersPanel"
import "./index.css"

const rootElement = document.getElementById("root")
if (!rootElement) {
  throw new Error("Missing #root element")
}

ReactDOM.createRoot(rootElement).render(
  <StrictMode>
    <TransfersPanel apiBaseUrl={import.meta.env.VITE_API_URL} />
  </StrictMode>,
)
