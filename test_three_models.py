import unittest
from unittest.mock import patch

import config
from market_pulse import (
    Article,
    SYSTEM_PROMPT,
    analyze_article_three,
    copilot_payload,
    copilot_reasoning_effort,
    copilot_request_url,
)


class ThreeModelTests(unittest.TestCase):
    def setUp(self):
        self.article = Article(
            id="test-1",
            title="Harmless market test",
            source="Test",
            link="https://example.com/test",
            published="2026-10-25",
            summary="No real market event.",
        )

    def test_aggregation_and_majority_notify(self):
        verdicts = [
            {"impact_score": 8, "summary": "one"},
            {"impact_score": 7, "summary": "two"},
            {"impact_score": 5, "summary": "three"},
        ]
        with patch("market_pulse.analyze_article", side_effect=verdicts):
            result = analyze_article_three(self.article, "token")
        self.assertEqual(result["impact_score"], 20 / 3)
        self.assertTrue(result["notify"])
        self.assertEqual([m["score"] for m in result["models"]], [8, 7, 5])

    def test_no_notify_when_only_one_reaches_threshold(self):
        verdicts = [
            {"impact_score": 8},
            {"impact_score": 5},
            {"impact_score": 4},
        ]
        with patch("market_pulse.analyze_article", side_effect=verdicts):
            result = analyze_article_three(self.article, "token")
        self.assertFalse(result["notify"])

    def test_two_available_both_must_meet_threshold(self):
        verdicts = [{"impact_score": 8}, {"impact_score": 7}, None]
        with patch("market_pulse.analyze_article", side_effect=verdicts):
            result = analyze_article_three(self.article, "token")
        self.assertTrue(result["notify"])
        self.assertEqual(len(result["models"]), 2)

        verdicts = [{"impact_score": 8}, {"impact_score": 5}, None]
        with patch("market_pulse.analyze_article", side_effect=verdicts):
            result = analyze_article_three(self.article, "token")
        self.assertFalse(result["notify"])

    def test_mai_uses_responses_payload(self):
        payload = copilot_payload(
            "mai-code-2-flash", "Reply briefly.", "OK", temperature=0.2
        )
        self.assertIn("input", payload)
        self.assertNotIn("messages", payload)
        self.assertTrue(copilot_request_url("mai-code-2-flash").endswith("/responses"))

    def test_anthropic_uses_chat_payload(self):
        payload = copilot_payload(
            "claude-opus-5.5", "Reply briefly.", "OK", temperature=0.2
        )
        self.assertIn("messages", payload)
        self.assertNotIn("input", payload)
        self.assertEqual(payload["reasoning_effort"], "high")
        self.assertEqual(payload["temperature"], 0.2)
        self.assertTrue(copilot_request_url("claude-opus-5.5").endswith("/chat/completions"))

    def test_gpt_uses_responses_payload(self):
        payload = copilot_payload(
            "gpt-6.1-sol", "Reply briefly.", "OK", temperature=0.2
        )
        self.assertIn("input", payload)
        self.assertNotIn("messages", payload)
        self.assertTrue(copilot_request_url("gpt-6.1-sol").endswith("/responses"))
        self.assertIn("json", payload["input"].lower())
        self.assertEqual(payload["reasoning"], {"effort": "medium"})
        self.assertNotIn("temperature", payload)

    def test_structured_responses_input_keeps_json_format(self):
        payload = copilot_payload(
            "gpt-6.1-sol",
            "Financial analysis.",
            "Analyze the headline.",
            temperature=0.2,
            json_response=True,
        )
        self.assertIn("json", payload["input"].lower())
        self.assertEqual(payload["text"], {"format": {"type": "json_object"}})

        payload = copilot_payload(
            "mai-code-2-flash",
            "Financial analysis.",
            "Analyze the headline.",
            temperature=0.2,
            json_response=True,
        )
        self.assertIn("json", payload["input"].lower())
        self.assertEqual(payload["text"], {"format": {"type": "json_object"}})
        self.assertEqual(payload["reasoning"], {"effort": "medium"})

    def test_financial_news_rubric_is_explicit(self):
        for phrase in (
            "Evidence/source quality",
            "Market mechanism",
            "Magnitude",
            "Persistence",
            "0-2",
            "3-4",
            "5-6",
            "7-8",
            "9-10",
            "rumors",
            "placeholder",
            "confidence",
        ):
            self.assertIn(phrase.lower(), SYSTEM_PROMPT.lower())

    def test_payload_shape_differs_by_endpoint(self):
        chat = copilot_payload(
            "claude-opus-5.5", SYSTEM_PROMPT, "Analyze this.", temperature=0.2,
            json_response=True,
        )
        responses = copilot_payload(
            "gpt-6.1-sol", SYSTEM_PROMPT, "Analyze this.", temperature=0.2,
            json_response=True,
        )
        self.assertIn("messages", chat)
        self.assertIn("reasoning_effort", chat)
        self.assertNotIn("reasoning", chat)
        self.assertIn("input", responses)
        self.assertIn("reasoning", responses)
        self.assertNotIn("reasoning_effort", responses)
        self.assertEqual(responses["text"], {"format": {"type": "json_object"}})

    def test_model_reasoning_defaults(self):
        self.assertEqual(copilot_reasoning_effort("claude-opus-5.5"), "high")
        self.assertEqual(copilot_reasoning_effort("gpt-6.1-sol"), "medium")
        self.assertEqual(copilot_reasoning_effort("mai-code-2-flash"), "medium")


if __name__ == "__main__":
    unittest.main()
