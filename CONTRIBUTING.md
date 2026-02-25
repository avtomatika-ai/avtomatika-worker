EN | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/CONTRIBUTING.md) | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/CONTRIBUTING.md)

# Contributing to Avtomatika Worker SDK

Thank you for helping improve the Shell of our ecosystem!

## Setup

1.  Clone the repository and navigate to this directory.
2.  Install dependencies:
    ```bash
    pip install -e .[test,s3,pydantic]
    ```

## Testing

Run the worker-specific tests:
```bash
pytest tests/
```

## Adding New Features

-   If adding a new configuration parameter, update `src/avtomatika_worker/config.py`.
-   If changing the protocol interaction, ensure compatibility with the `rxon` package.
-   Always update the `README.md` if the user-facing API changes.
