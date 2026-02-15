import pytest

from avtomatika_worker import SkillBlueprint, Worker


def test_blueprint_registration():
    """Tests that SkillBlueprint correctly collects tasks."""
    bp = SkillBlueprint()

    @bp.task("bp_task", task_type="gpu")
    def my_handler(params):
        return {"status": "success"}

    assert len(bp._tasks) == 1
    assert bp._tasks[0][0] == "bp_task"
    assert bp._tasks[0][1] == "gpu"
    assert bp._tasks[0][2] == my_handler


def test_worker_include_blueprint():
    """Tests that Worker correctly registers tasks from a blueprint."""
    worker = Worker()
    bp = SkillBlueprint()

    @bp.task("bp_task")
    def my_handler(params):
        pass

    worker.include_blueprint(bp)

    assert "bp_task" in worker._task_handlers
    assert worker._task_handlers["bp_task"]["func"] == my_handler


@pytest.mark.asyncio
async def test_dynamic_skill_loading(tmp_path, monkeypatch):
    """Tests that Worker dynamically loads skills from a directory."""
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()

    # Create a skill file with a SkillBlueprint
    skill_file_content = """
from avtomatika_worker import SkillBlueprint

bp = SkillBlueprint()

@bp.task("dynamic_task")
def dynamic_handler(params):
    return {"status": "dynamic_ok"}
"""
    (skills_dir / "my_skill.py").write_text(skill_file_content)

    # Create a skill file with a setup(worker) function
    setup_skill_content = """
def setup(worker):
    @worker.task("setup_task")
    def setup_handler(params):
        return {"status": "setup_ok"}
"""
    (skills_dir / "setup_skill.py").write_text(setup_skill_content)

    # Initialize worker with the custom skills directory
    monkeypatch.setenv("WORKER_SKILLS_DIR", str(skills_dir))
    worker = Worker()
    await worker.load_skills()

    assert "dynamic_task" in worker._task_handlers
    assert "setup_task" in worker._task_handlers

    # Verify they actually work
    assert worker._task_handlers["dynamic_task"]["func"](None) == {"status": "dynamic_ok"}
    assert worker._task_handlers["setup_task"]["func"](None) == {"status": "setup_ok"}


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
