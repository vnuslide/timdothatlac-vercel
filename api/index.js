import express from "express";
import bodyParser from "body-parser";
import cors from "cors";

const app = express();
app.use(bodyParser.json());
app.use(cors());

// Kiểm tra API hoạt động
app.get("/", (req, res) => {
  res.json({ message: "Vercel API is running!" });
});

// API Chat AI demo
app.post("/chat", async (req, res) => {
  try {
    const { question } = req.body;

    if (!question) {
      return res.status(400).json({ error: "Missing 'question' field" });
    }

    // Tạm trả lời cứng để test
    return res.json({
      answer: "AI trả lời: " + question,
    });
  } catch (err) {
    res.status(500).json({ error: "Server error", detail: err.toString() });
  }
});

// Export (quan trọng cho Vercel)
export default app;

