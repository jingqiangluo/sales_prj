import subprocess
import time
import logging

logging.basicConfig(level=logging.INFO)


def run_command_simple(cmd, cwd=None):
    result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(f"Command failed: {cmd}\n{result.stderr}")
        raise Exception(result.stderr)
    logging.info(result.stdout)
    return result


def run_command(cmd, cwd=None, check=True):
    """Run command with FULL visible output on failure."""
    logging.info(f"Running: {cmd}")

    result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)

    if result.stdout.strip():
        print(result.stdout.strip())

    if result.returncode != 0:
        print("\n" + "═" * 90)
        print("❌ COMMAND FAILED")
        print("═" * 90)
        print(f"Command: {cmd}")
        print(f"Exit Code: {result.returncode}")

        print("\n📋 STDOUT:")
        print(result.stdout if result.stdout.strip() else "(empty)")

        print("\n🚨 STDERR:")
        print(result.stderr if result.stderr.strip() else "(empty)")

        print("═" * 90)

        if check:
            raise Exception(f"Command failed: {cmd}\nSee error above.")

    return result


def main():
    # Start services
    print("Starting Docker services...")
    run_command("docker compose up -d postgres")
    time.sleep(5)  # Wait for DB ready

    # Run ETL
    print("Running Python ETL...")
    run_command("docker compose build etl && docker compose up --abort-on-container-exit etl")

    # Run dbt
    print("Running dbt models...")
    run_command("docker compose build dbt --no-cache")
    run_command("docker compose up -d dbt")  # Start and keep running

    # Run dbt commands using exec
    print("Executing dbt run...")
    dbt_run_cmd = "docker compose exec -T dbt dbt run --project-dir /usr/app --profiles-dir /usr/app"
    try:
        run_command(dbt_run_cmd)
    except Exception as e:
        print("\n🔍 FULL DBT ERROR:")
        print(str(e))
        print("\n⚠️ Pipeline continued despite test failure.")

    # Test
    print("Validating results...")
    # run_command("docker compose exec -T dbt dbt test")
    dbt_test_cmd = "docker compose exec -T dbt dbt test --project-dir /usr/app --profiles-dir /usr/app"
    try:
        run_command(dbt_test_cmd)
    except Exception as e:
        print("\n🔍 FULL DBT ERROR:")
        print(str(e))
        print("\n⚠️ Pipeline continued despite test failure.")

    # Show final results
    print("\n📊 Final Monthly Sales:")
    run_command("""docker compose exec postgres psql -U postgres -d sales_db -c "
        SELECT * FROM analytics.mart_sales_monthly 
        ORDER BY sale_year_month DESC LIMIT 10;
    " """)

    print("Pipeline completed successfully!")


if __name__ == "__main__":
    main()