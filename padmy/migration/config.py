import json
from dataclasses import dataclass, asdict
from padmy.env import PADMY_CONFIG
from .utils import get_git_email


@dataclass
class MigrationConfig:
    author: str

    @property
    def data(self):
        return asdict(self)

    @classmethod
    def load(cls):
        _data = json.loads(PADMY_CONFIG.read_text()) if PADMY_CONFIG.exists() else {}
        return cls(author=_data.get("author") or get_git_email())

    def save(self):
        if not PADMY_CONFIG.parent.exists():
            PADMY_CONFIG.parent.mkdir(parents=True)
        with PADMY_CONFIG.open("w") as f:
            json.dump(self.data, f, indent=4)
