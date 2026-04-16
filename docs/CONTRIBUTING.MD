# MetroPulse Team Development Guidelines

This document outlines the standard development workflow and rules to maintain code quality and organization.

---

## 1. Branching Strategy

We follow a structured branching model to ensure stability across the project:

*   **`main`**: Production-ready code only. **Direct commits to main are strictly prohibited.**
*   **`dev`**: Integration branch. All features and fixes are merged here for compatibility testing.
*   **`feature/<name>`**: For new tasks or features (e.g., `feature/spark-cleaning`, `feature/kafka-producer`).
*   **`fix/<name>`**: For urgent bug fixes (e.g., `fix/api-connection-timeout`).

---

## 2. Commit Message Convention

We adhere to the [Conventional Commits](https://www.conventionalcommits.org/) standard. This helps keep our git history clean. 
Format: `<type>(<scope>): <description>`

*   **`feat`**: New feature
*   **`fix`**: Bug fix
*   **`docs`**: Documentation updates (e.g., README, this file)
*   **`refactor`**: Code optimization without changing functionality
*   **`chore`**: Routine tasks, dependency updates (e.g., updating .gitignore, requirements)

**Example:** `feat(ingestion): add weather api producer script`

---

## 3. Pull Request (PR) Workflow

When you finish a task on your branch, please follow these steps:

1.  **Sync**: Pull the latest code from `dev` into your personal branch and resolve any conflicts.
2.  **Push**: Push your updated branch up to GitHub.
3.  **Create PR**: Open a Pull Request to merge your branch into `dev`.
4.  **Review (Mandatory)**: At least **one other team member** must review and **Approve** the PR before you can merge it.
5.  **Environment Updates**: Always remember to update `requirements.txt` or `docker-compose.yml` if your work introduces new libraries or services.
