# 🐍 Python Coding Interview Questions

Welcome to the **Python Coding Interview** repository!

## 📁 Project Structure

```
.
├── main.py           # Implement your solutions here
├── test.py           # Unit tests to verify your solutions
├── requirements.txt  # Python packages for virtual environment
└── README.md         # Project documentation
```

## 🛠 Prerequisites

Make sure you have the following installed:

- **Python 3.7+**
- **venv module** (comes bundled with Python 3.3+)

To check your Python version:

```bash
python3 --version
```

##### Make sure you have an SSH key for the device you will use associated with your GitHub account!

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone git@github.com:lys-jcole/interview-python.git
cd interview-python
```

### 2. Set Up Virtual Environment

It’s recommended to use a virtual environment:

```bash
python -m venv venv
source venv/bin/activate    # On Windows use: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Solve the Problems

Open `main.py` and complete the function implementations.

Each function includes a docstring describing the expected behavior, inputs, and outputs.

### 4. Run the Tests

To verify your solutions, run the test suite:

```bash
pytest -v test.py
```

All tests must pass for your implementation to be considered correct.

## 📦 Requirements

All dependencies are listed in `requirements.txt`. It includes:

```
pytest
pandas
```

The pandas library is required for the ETL problem which tests real-world data transformation skills.

## ✅ Tips

- Read the problem carefully before implementing.
- Write clean, readable, and efficient code.
- Run tests frequently to catch errors early.
- Don't be afraid to refactor!
- **Python documentation is available at:**
  - **Official Python Documentation:** https://docs.python.org/3/
  - **W3Schools Python Tutorial:** https://www.w3schools.com/python/
  - **PyDocs Reference:** https://www.pydocs.site/en/

## ⚠️ Important Rules

- **Screen sharing is required:** You must share your entire screen during the interview.
- **AI coding assistants (GitHub Copilot, ChatGPT, etc.) are strictly forbidden during this interview.**
- **Any suspicion of using AI tools or cheating will result in immediate termination of the interview.**
- You may use the Python documentation and search for syntax references online.

---

Happy coding and good luck with your interviews! 🚀
