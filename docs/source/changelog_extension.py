import re
from pathlib import Path
from sphinx.util import logging

logger = logging.getLogger(__name__)

PROJECT_ROOT = (Path(__file__).parent / '..' / '..').resolve()
CHANGELOG_FILE = PROJECT_ROOT / "CHANGELOG.md"
SOURCE_DIR = Path(__file__).parent
OUTPUT_DIR = SOURCE_DIR / "release_notes"
MAIN_INDEX = OUTPUT_DIR / "index.rst"

VERSION_HEADER_RE = re.compile(r"^## \[(.*?)\]\s*-\s*(\d{4}-\d{2}-\d{2})$")
SECTION_HEADER_RE = re.compile(r"^###\s+(.*)$")
BULLET_RE = re.compile(r"^\s*[-*]\s*(.*)$")
CODE_SNIPPET_RE = re.compile(r"`([^`]+)`")

INTRO_TEXT = (
    "All notable changes to this project will be documented here.\n\n"
    "The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.1.0/>`_,\n"
    "and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.\n\n"
)

def escape_inline_backticks(text):
    text = CODE_SNIPPET_RE.sub(r"``\1``", text)
    return re.sub(r"(?<!`)`(?!`)", r"\`", text)

def parse_changelog():
    if not CHANGELOG_FILE.exists():
        logger.warning("CHANGELOG.md not found at %s", CHANGELOG_FILE)
        return []

    lines = CHANGELOG_FILE.read_text(encoding="utf-8").splitlines(keepends=True)
    entries = []
    current_version, current_date, buffer = None, None, []

    for line in lines:
        m = VERSION_HEADER_RE.match(line.strip())
        if m:
            if current_version:
                entries.append(((current_version, current_date), buffer))
            current_version, current_date = m.groups()
            title = f"{current_version} ({current_date})"
            separator = "-" * len(title)
            buffer = [f"{title}\n", f"{separator}\n\n"]
        elif current_version:
            sec = SECTION_HEADER_RE.match(line)
            if sec:
                title = sec.group(1)
                buffer.extend([f"{title}\n", f"{'~' * len(title)}\n\n"])
            else:
                bm = BULLET_RE.match(line)
                if bm:
                    text = escape_inline_backticks(bm.group(1))
                    buffer.append(f"* {text}\n")
                else:
                    text = escape_inline_backticks(line)
                    buffer.append(text)

    if current_version:
        entries.append(((current_version, current_date), buffer))
    return entries

def generate_changelog_index(app):
    entries = parse_changelog()
    if not entries:
        return

    grouped = {}
    for (ver, date), buf in entries:
        major = ver.split('.')[0]
        key = f"v{major}"
        grouped.setdefault(key, []).append((ver, date, buf))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for major, version_entries in grouped.items():
        major_dir = OUTPUT_DIR / major
        major_dir.mkdir(parents=True, exist_ok=True)

        version_entries.sort(key=lambda x: tuple(map(int, x[0].split('.'))), reverse=True)

        index_path = major_dir / "index.rst"
        with index_path.open("w", encoding="utf-8") as f:
            label = f"changelog_{major.lower()}"
            f.write(f".. _{label}:\n\n")
            f.write(f"{major.upper()} Changelog\n")
            f.write(f"{'=' * (len(major) + 10)}\n\n")
            for _ver, _date, buf in version_entries:
                for line in buf:
                    f.write(line)
                f.write("\n")

    # Main index.rst with :ref: list and hidden toctree
    with MAIN_INDEX.open("w", encoding="utf-8") as f:
        f.write("Changelog\n")
        f.write("=========\n\n")
        f.write(INTRO_TEXT)
        f.write("What you'll find here:\n\n")
        for major in sorted(grouped.keys(), reverse=True):
            label = f"changelog_{major.lower()}"
            title = f"{major.upper()} changelog"
            f.write(f"- :ref:`{label}` - {title}\n")
        f.write("\n.. toctree::\n")
        f.write("   :maxdepth: 1\n")
        f.write("   :hidden:\n\n")
        for major in sorted(grouped.keys(), reverse=True):
            f.write(f"   {major}/index\n")

def setup(app):
    app.connect('builder-inited', generate_changelog_index)
    return {
        "version": "1.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True
    }
