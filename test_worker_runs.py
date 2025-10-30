#!/usr/bin/env python3
"""
Test Worker Runs - Execute worker_scheduled.py for each view
This script runs the actual worker for each view to test end-to-end processing
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

# View configurations
VIEW_CONFIGS = {
    'sol_1000_swaps_3_days': {
        'description': '1000+ swaps in 3 days',
        'schedule': 'Daily at 00:00 UTC',
        'log_file': 'test-1000-swaps-3d.log'
    },
    'sol_500_swaps_7_days': {
        'description': '500+ swaps in 7 days',
        'schedule': 'Every 5 minutes',
        'log_file': 'test-500-swaps-7d.log'
    },
    'sol_100_swaps_30_days': {
        'description': '100+ swaps in 30 days',
        'schedule': 'Daily at 00:10 UTC',
        'log_file': 'test-100-swaps-30d.log'
    }
}


def run_worker_for_view(view_name: str, save_logs: bool = False) -> tuple:
    """
    Run worker_scheduled.py for a specific view

    Args:
        view_name: Name of the view to process
        save_logs: Whether to save logs to file

    Returns:
        Tuple of (return_code, stdout, stderr)
    """
    view_config = VIEW_CONFIGS[view_name]

    print("\n" + "=" * 100)
    print(f"🚀 RUNNING WORKER FOR: {view_name}")
    print("=" * 100)
    print(f"Description: {view_config['description']}")
    print(f"Schedule: {view_config['schedule']}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100 + "\n")

    # Build command
    cmd = [
        sys.executable,  # Use same Python interpreter
        'worker_scheduled.py',
        '--view', view_name
    ]

    try:
        # Run worker and capture output
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )

        # Print output
        if result.stdout:
            print(result.stdout)

        if result.stderr:
            print("\n⚠️  STDERR:", file=sys.stderr)
            print(result.stderr, file=sys.stderr)

        # Save logs if requested
        if save_logs:
            log_dir = Path('logs')
            log_dir.mkdir(exist_ok=True)
            log_file = log_dir / view_config['log_file']

            with open(log_file, 'a') as f:
                f.write(f"\n{'=' * 100}\n")
                f.write(f"Test run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"View: {view_name}\n")
                f.write(f"{'=' * 100}\n\n")
                f.write(result.stdout)
                if result.stderr:
                    f.write(f"\n\nSTDERR:\n{result.stderr}\n")
                f.write(f"\n{'=' * 100}\n\n")

            print(f"\n📝 Logs saved to: {log_file}")

        # Print summary
        print("\n" + "-" * 100)
        print(f"✅ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Return code: {result.returncode}")

        if result.returncode == 0:
            print("Status: SUCCESS ✓")
        else:
            print(f"Status: FAILED ✗")

        print("-" * 100 + "\n")

        return (result.returncode, result.stdout, result.stderr)

    except subprocess.TimeoutExpired:
        print(f"\n❌ ERROR: Worker timed out after 1 hour")
        return (1, "", "Timeout")

    except Exception as e:
        print(f"\n❌ ERROR: Failed to run worker: {e}")
        import traceback
        traceback.print_exc()
        return (1, "", str(e))


def run_all_workers(save_logs: bool = False, stop_on_error: bool = False):
    """
    Run worker for all views sequentially

    Args:
        save_logs: Whether to save logs to files
        stop_on_error: Whether to stop if a worker fails
    """
    print("\n" + "=" * 100)
    print("🔄 RUNNING WORKERS FOR ALL VIEWS")
    print("=" * 100)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total views: {len(VIEW_CONFIGS)}")
    print("=" * 100 + "\n")

    results = {}

    for view_name in VIEW_CONFIGS.keys():
        return_code, stdout, stderr = run_worker_for_view(view_name, save_logs)
        results[view_name] = {
            'return_code': return_code,
            'success': return_code == 0,
            'stdout': stdout,
            'stderr': stderr
        }

        # Stop on error if requested
        if stop_on_error and return_code != 0:
            print(f"\n❌ Stopping due to error in {view_name}")
            break

    # Print summary
    print("\n" + "=" * 100)
    print("📊 SUMMARY - ALL WORKERS")
    print("=" * 100 + "\n")

    print(f"{'View':<30} {'Status':<15} {'Description':<50}")
    print("-" * 100)

    for view_name, result in results.items():
        status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
        description = VIEW_CONFIGS[view_name]['description']
        print(f"{view_name:<30} {status:<15} {description:<50}")

    successful = sum(1 for r in results.values() if r['success'])
    failed = len(results) - successful

    print("-" * 100)
    print(f"Total: {len(results)} | Successful: {successful} | Failed: {failed}")
    print("=" * 100 + "\n")

    return results


def test_worker_dry_run(view_name: str):
    """
    Dry run - just list what would be processed without running worker

    Args:
        view_name: Name of the view to test
    """
    print("\n" + "=" * 100)
    print(f"🔍 DRY RUN FOR: {view_name}")
    print("=" * 100 + "\n")

    # Run test_views.py to show tokens
    cmd = [
        sys.executable,
        'test_views.py',
        '--view', view_name,
        '--limit', '10'
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
    except Exception as e:
        print(f"ERROR: {e}")

    print("\n💡 To run actual worker, use:")
    print(f"   python test_worker_runs.py --view {view_name}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Test Worker Runs - Execute worker_scheduled.py for each view',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available Views:
  sol_1000_swaps_3_days   - Tokens with 1000+ swaps in last 3 days
  sol_500_swaps_7_days    - Tokens with 500+ swaps in last 7 days
  sol_100_swaps_30_days   - Tokens with 100+ swaps in last 30 days

Examples:
  # Run worker for single view
  python test_worker_runs.py --view sol_500_swaps_7_days

  # Run worker and save logs
  python test_worker_runs.py --view sol_500_swaps_7_days --save-logs

  # Run workers for all views
  python test_worker_runs.py --all

  # Run all with log saving
  python test_worker_runs.py --all --save-logs

  # Dry run (just show tokens, don't process)
  python test_worker_runs.py --view sol_500_swaps_7_days --dry-run

  # Run all, stop on first error
  python test_worker_runs.py --all --stop-on-error
        """
    )

    parser.add_argument(
        '--view',
        type=str,
        choices=list(VIEW_CONFIGS.keys()),
        help='Run worker for specific view'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Run workers for all views sequentially'
    )

    parser.add_argument(
        '--save-logs',
        action='store_true',
        help='Save logs to files in logs/ directory'
    )

    parser.add_argument(
        '--stop-on-error',
        action='store_true',
        help='Stop processing if a worker fails (only with --all)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run - show tokens without processing'
    )

    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available views and exit'
    )

    args = parser.parse_args()

    # List views
    if args.list:
        print("\nAvailable Views:")
        print("=" * 100)
        for view_name, config in VIEW_CONFIGS.items():
            print(f"\n{view_name}")
            print(f"  Description: {config['description']}")
            print(f"  Schedule: {config['schedule']}")
            print(f"  Test log file: logs/{config['log_file']}")
        print("\n")
        return

    # Dry run for single view
    if args.dry_run and args.view:
        test_worker_dry_run(args.view)
        return

    # Run all workers
    if args.all:
        results = run_all_workers(args.save_logs, args.stop_on_error)
        # Exit with error code if any worker failed
        if any(not r['success'] for r in results.values()):
            sys.exit(1)
        return

    # Run single worker
    if args.view:
        return_code, _, _ = run_worker_for_view(args.view, args.save_logs)
        sys.exit(return_code)

    # No arguments - show help
    parser.print_help()


if __name__ == '__main__':
    main()
