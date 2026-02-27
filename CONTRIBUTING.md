# Contributing to MovieData ETL Pipeline

We welcome contributions to the MovieData ETL Pipeline project! By participating in this project, you agree to abide by our Code of Conduct. This document outlines the process for contributing to this project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Enhancements](#suggesting-enhancements)
  - [Submitting Pull Requests](#submitting-pull-requests)
- [Development Setup](#development-setup)
- [Style Guides](#style-guides)
  - [Git Commit Messages](#git-commit-messages)
  - [Python Code Style](#python-code-style)
- [Additional Notes](#additional-notes)

## Code of Conduct

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.

## How Can I Contribute?

### Reporting Bugs

If you find a bug, please report it by opening an issue on GitHub. Provide as much detail as possible to help us understand and reproduce the issue. Include:

- A clear and descriptive title.
- A detailed description of the issue.
- Steps to reproduce the issue.
- Any relevant logs, screenshots, or stack traces.

### Suggesting Enhancements

If you have an idea for a new feature or an improvement, please open an issue to discuss it. Provide:

- A clear and descriptive title.
- A detailed description of the enhancement.
- The rationale for the enhancement and the benefits it would provide.
- Any relevant examples or mockups.

### Submitting Pull Requests

To contribute code to the project, follow these steps:

1. **Fork the repository** to your GitHub account.
2. **Clone your fork** to your local machine:
   ```bash
   git clone https://github.com/TheODDYSEY/YTS-Pipeline-Postgres
   cd YTS-Pipeline-Postgres

   ```
3. **Create a new branch** for your feature or bugfix:
   ```bash
   git checkout -b my-feature-branch
   ```
4. **Make your changes** and commit them with clear and descriptive commit messages.
5. **Push your branch** to your forked repository:
   ```bash
   git push origin my-feature-branch
   ```
6. **Open a pull request** on GitHub and provide a detailed description of your changes.

## Development Setup

To set up your development environment, follow these steps:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/TheODDYSEY/YTS-Pipeline-Postgres
   cd YTS-Pipeline-Postgres

   ```
2. **Install Docker** if you don't have it already. Follow the instructions [here](https://docs.docker.com/get-docker/).
3. **Build and run the Docker containers**:
   ```bash
   docker-compose up --build
   ```

## Style Guides

### Git Commit Messages

- Use the present tense ("Add feature" not "Added feature").
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...").
- Limit the first line to 72 characters or less.
- Reference issues and pull requests when applicable (e.g., `Fixes #123`).

### Python Code Style

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) for Python code.
- Use type hints as much as possible for better code readability and maintenance.
- Write docstrings for all public modules, functions, classes, and methods.

## Additional Notes

- **Tests**: Ensure your code passes existing tests and add new tests for new features or bug fixes.
- **Documentation**: Update the documentation to reflect your changes, including the README and any relevant comments in the code.
- **Reviews**: Be prepared to address feedback during the review process. We appreciate your patience and effort in making this project better!

Thank you for contributing to MovieData ETL Pipeline! Together, we can build something amazing. 🚀
