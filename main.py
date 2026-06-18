from core.engine import run_engine

if __name__ == "__main__":
    run_engine(
        project="CLAIMS",
        process="MEMBER",
        run_type="MONTHLY",
        run_mode="BATCH",
    )
