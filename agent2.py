import os
import sys
import json
import pandas as pd
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from tools.validation_executor_tool import ValidationExecutorTool
from tools.profile_csv_generator import ProfileCSVGeneratorTool


class LLMFreeDQExecutionAgent:
    """
    Direct execution of DQ validation and profiling without LLM dependencies.
    """
    
    def __init__(self, file_path, output_dir="outputs"):
        self.file_path = file_path
        self.output_dir = Path(output_dir)
        
        # Create output directories
        (self.output_dir / "issues").mkdir(parents=True, exist_ok=True)
        (self.output_dir / "profiles").mkdir(parents=True, exist_ok=True)
        
        # Initialize tools
        print("\n📦 Initializing Agent 2 tools...")
        
        # Extract tool functions
        self.validation_tool = ValidationExecutorTool(file_path=file_path)
        self.profile_csv_tool = ProfileCSVGeneratorTool()
        
        print("   ✓ Validation Execution Tool")
        print("   ✓ Profile CSV Generation Tool")
    
    def verify_prerequisites(self):
        """Verify that Agent 1 outputs exist"""
        print("\n✅ Checking for Agent 1 outputs...")
        
        required_files = [
            self.output_dir / "quality_validation_instructions.yaml",
            self.output_dir / "profiling_results.json"
        ]
        
        missing_files = []
        for req_file in required_files:
            if not req_file.exists():
                missing_files.append(str(req_file))
                print(f"   ❌ Missing: {req_file}")
            else:
                print(f"   ✓ Found: {req_file}")
        
        if missing_files:
            raise FileNotFoundError(
                f"Agent 1 outputs not found! Run Agent 1 first to generate:\n" +
                "\n".join(f"   - {f}" for f in missing_files)
            )
        
        # Verify Excel file
        if not Path(self.file_path).exists():
            raise FileNotFoundError(f"Excel file not found: {self.file_path}")
        
        print(f"   ✓ Found: {self.file_path}")
    
    def execute_validation(self):
        """Step 1: Execute validation rules and find all issues"""
        print("\n" + "="*80)
        print("STEP 1: Execute Validation Rules")
        print("="*80)
        
        rules_file = str(self.output_dir / "quality_validation_instructions.yaml")
        
        try:
            # Execute the validation tool directly
            result = self.validation_tool._run(
                file_path=self.file_path,
                rules_file=rules_file,
                output_dir=str(self.output_dir)
            )
            
            print(f"\n✅ Validation complete")
            print(f"   Issue CSVs saved to: {self.output_dir / 'issues'}/")
            print(f"   Combined report: {self.output_dir / 'quality_issues_with_remediation.csv'}")
            
            return result
            
        except Exception as e:
            print(f"❌ Validation execution failed: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def generate_profile_csvs(self):
        """Step 2: Generate profile CSVs from JSON"""
        print("\n" + "="*80)
        print("STEP 2: Generate Profile CSVs")
        print("="*80)
        
        profiling_json = str(self.output_dir / "profiling_results.json")
        profile_output_dir = str(self.output_dir / "profiles")
        
        try:
            # Execute the profile CSV generation tool directly
            result = self.profile_csv_tool._run(
                profiling_json=profiling_json,
                output_dir=profile_output_dir
            )
            
            print(f"✅ Profile CSVs generated")
            print(f"   Saved to: {profile_output_dir}/")
            
            return result
            
        except Exception as e:
            print(f"❌ Profile CSV generation failed: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def print_execution_summary(self):
        """Print summary of generated outputs"""
        print("\n" + "="*80)
        print("📊 EXECUTION SUMMARY")
        print("="*80)
        
        # Profile CSVs
        print("\n📈 Profile CSVs:")
        profile_dir = self.output_dir / "profiles"
        if profile_dir.exists():
            profile_files = sorted(profile_dir.glob("*_profile.csv"))
            if profile_files:
                for f in profile_files:
                    file_size = f.stat().st_size
                    print(f"   ✓ {f.name} ({file_size:,} bytes)")
            else:
                print("   (No profile files generated)")
        
        # Issue CSVs
        print("\n🔍 Issue CSVs by Entity:")
        issues_dir = self.output_dir / "issues"
        total_issues = 0
        
        if issues_dir.exists():
            issue_files = sorted(issues_dir.glob("*_issues.csv"))
            if issue_files:
                for f in issue_files:
                    try:
                        df = pd.read_csv(f)
                        entity_name = f.stem.replace("_issues", "")
                        total_issues += len(df)
                        print(f"   ❌ {entity_name}: {len(df)} issues")
                    except Exception as e:
                        print(f"   ⚠️ Could not read {f.name}: {e}")
            else:
                print("   ✅ No issue files - all entities are clean!")
        
        # Combined report
        print("\n📋 Combined Report:")
        combined_file = self.output_dir / "quality_issues_with_remediation.csv"
        
        if combined_file.exists():
            try:
                # ✅ FIX: Add small delay to ensure file is fully written
                import time
                time.sleep(0.5)
                
                df = pd.read_csv(combined_file)
                print(f"   ✓ quality_issues_with_remediation.csv ({len(df)} total issues)")
                
                if len(df) > 0:
                    # Severity breakdown
                    print("\n   📊 Issue Severity Breakdown:")
                    if 'severity' in df.columns:
                        severity_counts = df['severity'].value_counts()
                        for severity in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
                            if severity in severity_counts.index:
                                count = severity_counts[severity]
                                percentage = (count / len(df)) * 100
                                print(f"      • {severity}: {count} ({percentage:.1f}%)")
                    
                    # Top issue types
                    print("\n   🔍 Top Issue Types:")
                    if 'rule_violated' in df.columns:
                        rule_counts = df['rule_violated'].value_counts().head(5)
                        for rule, count in rule_counts.items():
                            print(f"      • {rule}: {count}")
                    
                    # Entity breakdown
                    print("\n   📊 Issues by Entity:")
                    if 'entity' in df.columns:
                        entity_counts = df['entity'].value_counts()
                        for entity, count in entity_counts.items():
                            percentage = (count / len(df)) * 100
                            print(f"      • {entity}: {count} ({percentage:.1f}%)")
                else:
                    print("\n   ✅ Combined report is empty - all data is clean!")
                
            except PermissionError:
                print(f"   ⚠️ File is currently OPEN in Excel/editor")
                print(f"   🚨 Please CLOSE the file and check it manually")
                print(f"   📁 Location: {combined_file}")
            
            except Exception as e:
                print(f"   ⚠️ Could not read file: {e}")
                print(f"   📁 File location: {combined_file}")
        else:
            print("   ℹ️ No combined report found")
        
        print("\n" + "="*80)
        print(f"📁 ALL FILES SAVED IN: {self.output_dir.absolute()}/")
        print(f"   - {self.output_dir}/issues/ (per-entity issue CSVs)")
        print(f"   - {self.output_dir}/profiles/ (entity profile CSVs)")
        print(f"   - {self.output_dir}/quality_issues_with_remediation.csv (combined report)")
        print("="*80)
    
    def run(self):
        """Execute complete DQ validation and profiling pipeline"""
        print("="*80)
        print("AGENT 2: DQ EXECUTION & ENRICHMENT EXPERT (NO LLM)")
        print("="*80)
        print(f"Input file: {self.file_path}")
        print(f"Output directory: {self.output_dir}")
        
        try:
            # Verify prerequisites
            self.verify_prerequisites()
            
            # Step 1: Execute validation rules
            validation_result = self.execute_validation()
            
            # Step 2: Generate profile CSVs
            profile_result = self.generate_profile_csvs()
            
            # ✅ FIX: Add small delay before reading summary
            import time
            time.sleep(1)
            
            # Print summary
            self.print_execution_summary()
            
            print("\n" + "="*80)
            print("✅ AGENT 2 EXECUTION COMPLETE (LLM-FREE)")
            print("="*80)
            print("\nNext Steps:")
            print("  1. Review per-entity issue CSVs in outputs/issues/")
            print("  2. Check profile CSVs in outputs/profiles/")
            print("  3. Analyze combined report: quality_issues_with_remediation.csv")
            print("  4. Proceed to Agent 3 for reporting and analysis")
            
            return {
                "status": "success",
                "validation": validation_result,
                "profiles": profile_result
            }
            
        except Exception as e:
            print("\n" + "="*80)
            print("❌ AGENT 2 EXECUTION FAILED")
            print("="*80)
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            raise


def run_agent_2(file_path, output_dir="outputs"):
    """
    Main entry point for Agent 2 - completely LLM-free execution
    """
    # Create and run agent
    agent = LLMFreeDQExecutionAgent(file_path, output_dir)
    result = agent.run()
    
    return result


if __name__ == "__main__":
    # ✅ UPDATE THIS PATH to your actual Excel file location
    file_path = r"D:\Muskan.Verma_OneDrive_Data\OneDrive - Course5 Intelligence Limited\Desktop\dqm_agentic_system\data\DQ_dataset.xlsx"  # ✅ CORRECT!
    
    print("\n" + "="*80)
    print("RUNNING AGENT 2: DQ EXECUTION & ENRICHMENT EXPERT (LLM-FREE)")
    print("="*80)
    
    try:
        result = run_agent_2(file_path, output_dir="outputs")
        
        print("\n✅ Agent 2 execution completed successfully!")
        print("\n📄 Result Summary:")
        print(json.dumps(result, indent=2, default=str))
        
    except Exception as e:
        print(f"\n❌ Agent 2 execution failed: {e}")
        import traceback
        traceback.print_exc()
