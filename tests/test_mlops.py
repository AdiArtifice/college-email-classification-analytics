"""
Unit tests for MLOps module.
Verifies MLflow tracking retrieval, Model Registry inspection,
live email inference, and concept drift analysis.
"""

import unittest
import os
import sys

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from mlops.tracker import get_experiment_runs, get_model_registry_info
from mlops.inference import predict_email
from mlops.drift import analyze_drift


class TestMLOpsModule(unittest.TestCase):

    def test_01_experiment_runs_retrieval(self):
        """Verify that MLflow recorded runs can be retrieved as a DataFrame."""
        runs_df = get_experiment_runs("College_Email_Classification")
        self.assertFalse(runs_df.empty, "MLflow runs DataFrame should not be empty")
        self.assertIn("params.model_type", runs_df.columns)
        self.assertIn("metrics.f1_score", runs_df.columns)
        self.assertIn("metrics.accuracy", runs_df.columns)

    def test_02_model_registry_info(self):
        """Verify that CollegeEmailClassifier exists in Model Registry."""
        reg_info = get_model_registry_info("CollegeEmailClassifier")
        self.assertEqual(reg_info["name"], "CollegeEmailClassifier")
        self.assertGreaterEqual(len(reg_info["versions"]), 1)
        # Check production version exists
        stages = [v["current_stage"] for v in reg_info["versions"]]
        self.assertIn("Production", stages)

    def test_03_inference_internship(self):
        """Verify live inference on internship email."""
        res = predict_email(
            subject="Summer Internship 2026 - AI/ML Engineer",
            body="Applications are invited for software and data engineering virtual internships. Stipend: Rs 40,000. Apply before 2026-10-30.",
            sender_domain="eduskillsfoundation.org"
        )
        self.assertEqual(res["category"], "Internship")
        self.assertGreater(res["confidence"], 0.8)
        self.assertEqual(res["organization"], "EduSkills Foundation")
        self.assertEqual(res["deadline"], "2026-10-30")

    def test_04_inference_hackathon(self):
        """Verify live inference on hackathon email."""
        res = predict_email(
            subject="Smart India Hackathon 2026 - Registration Open",
            body="Form your team and submit your prototype for the 36-hour national coding competition and hackathon.",
            sender_domain="unstop.com"
        )
        self.assertEqual(res["category"], "Hackathon/Competition")
        self.assertGreater(res["confidence"], 0.8)
        self.assertEqual(res["organization"], "Unstop")

    def test_05_inference_examination(self):
        """Verify live inference on examination notice."""
        res = predict_email(
            subject="Timetable for KT and End Semester Examinations",
            body="Students appearing for regular and backlog exams must check the attached hall ticket and schedule.",
            sender_domain="sjcem.edu.in"
        )
        self.assertEqual(res["category"], "Examination")
        self.assertGreater(res["confidence"], 0.8)

    def test_06_drift_analysis(self):
        """Verify drift and observability metrics on processed dataset."""
        drift_res = analyze_drift()
        self.assertEqual(drift_res["status"], "healthy")
        self.assertEqual(drift_res["total_records"], 7276)
        self.assertGreater(drift_res["overall_avg_confidence"], 0.95)
        self.assertIn("drift_status", drift_res)


if __name__ == '__main__':
    unittest.main()
