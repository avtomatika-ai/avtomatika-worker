import pytest

from avtomatika_worker import SkillBlueprint, Worker


def test_blueprint_registration():
    """Tests that SkillBlueprint correctly collects tasks."""
    bp = SkillBlueprint()

    @bp.skill("bp_task", type="gpu")
    def my_handler(params):
        return {"status": "success"}

    assert len(bp._skills) == 1
    skill_info, handler = bp._skills[0]
    assert skill_info.name == "bp_task"
    assert skill_info.type == "gpu"
    assert handler == my_handler


def test_worker_include_blueprint():
    """Tests that Worker correctly registers tasks from a blueprint."""
    worker = Worker()
    bp = SkillBlueprint()

    @bp.skill("bp_task")
    def my_handler(params):
        pass

    worker.include_blueprint(bp)

    assert "bp_task" in worker._skill_handlers
    assert worker._skill_handlers["bp_task"]["func"] == my_handler


@pytest.mark.asyncio
async def test_dynamic_skill_loading(tmp_path, monkeypatch):
    """Tests that Worker dynamically loads skills from a directory."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()

    # Create a skill file with a SkillBlueprint
    skill_file_content = """
from avtomatika_worker import SkillBlueprint

bp = SkillBlueprint()

@bp.skill("dynamic_task")
def dynamic_handler(params):
    return {"status": "dynamic_ok"}
"""
    (skills_dir / "my_skill.py").write_text(skill_file_content)

    # Create a skill file with a setup(worker) function
    setup_skill_content = """
def setup(worker):
    @worker.skill("setup_task")
    def setup_handler(params):
        return {"status": "setup_ok"}
"""
    (skills_dir / "setup_skill.py").write_text(setup_skill_content)

    # Initialize worker with the custom skills directory
    monkeypatch.setenv("WORKER_SKILLS_DIR", str(skills_dir))
    worker = Worker()
    await worker.load_skills()

    assert "dynamic_task" in worker._skill_handlers
    assert "setup_task" in worker._skill_handlers

    # Verify they actually work
    assert worker._skill_handlers["dynamic_task"]["func"](None) == {"status": "dynamic_ok"}
    assert worker._skill_handlers["setup_task"]["func"](None) == {"status": "setup_ok"}


@pytest.mark.asyncio
async def test_dynamic_skill_loading_error_handling(tmp_path, monkeypatch, mocker):
    """Tests that loading errors are logged and don't crash the worker."""
    skills_dir = tmp_path / "broken_skills"
    skills_dir.mkdir()

    # Create a broken skill file
    (skills_dir / "broken.py").write_text("this is invalid python code")

    # Mock the logger in the worker module
    mock_logger = mocker.patch("avtomatika_worker.worker.logger")

    monkeypatch.setenv("WORKER_SKILLS_DIR", str(skills_dir))
    worker = Worker()
    await worker.load_skills()

    # Verify that error was called on the logger
    assert mock_logger.error.called
    args, _ = mock_logger.error.call_args
    assert "Failed to load skills from broken.py" in args[0]
