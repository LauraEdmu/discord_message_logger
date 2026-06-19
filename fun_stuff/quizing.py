import json
import re
import hashlib
from pathlib import Path


class QuizHandler:
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.questions = self._load_questions()
        self.quiz_id = self._get_quiz_id()
        self.progress_path = self._get_progress_path()

    def _load_questions(self) -> list[dict]:
        """
        Load quiz data from a JSONL file.
        """
        if not self.file_path.exists():
            return []

        questions = []

        with self.file_path.open("r", encoding="utf-8") as file:
            for line in file:
                if line.strip():
                    quiz_item = json.loads(line)
                    questions.append(quiz_item)

        return questions

    def _get_quiz_id(self) -> str:
        """
        Generate a stable quiz ID based on the quiz file contents.
        """
        if not self.file_path.exists():
            return ""

        hasher = hashlib.sha256()

        with self.file_path.open("rb") as file:
            for chunk in iter(lambda: file.read(8192), b""):
                hasher.update(chunk)

        return hasher.hexdigest()

    def _get_progress_path(self) -> Path:
        """
        Get the path to the progress file for this quiz.
        """
        return self.file_path.parent / f"{self.quiz_id}_quiz_progress.json"

    def _load_progress(self) -> dict[str, int]:
        """
        Load quiz progress data.
        """
        if not self.progress_path.exists():
            return {}

        with self.progress_path.open("r", encoding="utf-8") as file:
            return json.load(file)

    def _save_progress(self, data: dict[str, int]) -> None:
        """
        Save quiz progress data.
        """
        with self.progress_path.open("w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)

    def get_question_index(self, user_id: str) -> int:
        """
        Get the current question index for a user/guild.
        """
        if not self.questions:
            return -1

        data = self._load_progress()
        index = data.get(str(user_id), 0)

        # Keeps old progress safe if the quiz shrinks.
        return index % len(self.questions)

    def advance_question_index(self, user_id: str) -> None:
        """
        Advance the current question index for a user/guild.
        """
        if not self.questions:
            return

        user_id = str(user_id)

        data = self._load_progress()
        current_index = data.get(user_id, 0)
        new_index = (current_index + 1) % len(self.questions)

        data[user_id] = new_index
        self._save_progress(data)

    def reload_questions(self) -> None:
        """
        Reload quiz data from the JSONL file.

        Since the quiz ID is content-based, this also updates the progress path.
        """
        self.questions = self._load_questions()
        self.quiz_id = self._get_quiz_id()
        self.progress_path = self._get_progress_path()

    def get_question(self, index: int) -> dict:
        """
        Get a question by index.
        """
        if 0 <= index < len(self.questions):
            return self.questions[index]

        return {}

    def get_current_question(self, user_id: str) -> dict:
        """
        Get the current question for a user/guild.
        """
        index = self.get_question_index(user_id)
        return self.get_question(index)

    def check_answer(self, answer_str: str, question_index: int) -> tuple[bool, str]:
        """
        Check if an answer is correct for a given question index.

        Expected JSONL format:

        {"question": "Where is Paris?", "pattern": "france", "answer": "France"}
        {"question": "What is Luna?", "pattern": "moon|satellite", "answer": "A moon / satellite"}
        """
        question = self.get_question(question_index)

        if not question:
            return False, ""

        pattern = question.get("pattern", "")
        real_answer = question.get("answer", "")

        if not pattern or not real_answer:
            return False, ""

        is_correct = bool(re.search(pattern, answer_str, re.IGNORECASE))
        return is_correct, real_answer