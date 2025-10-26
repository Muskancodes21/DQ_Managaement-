"""
Agent 1: Generalized DQ Discovery Expert (LLM-FREE) - STANDALONE VERSION
Directly imports and uses the actual tool classes without crew_tools wrapper
"""

import os
import sys
import json
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

print(f"Project root: {project_root}")

# Direct imports of tool classes
try:
    from tools.profiling_tool import DataProfilingTool
    from tools.rule_generator_tool import RuleGeneratorTool
    print("✅ Successfully imported tools directly")
except ImportError as e:
    print(f"❌ Import Error: {e}")
    print("\nMake sure these files exist:")
    print("  - tools/data_profiling_tool.py")
    print("  - tools/rule_generator_tool.py")
    raise


class LLMFreeDQAgent:
    """
    Direct execution of DQ tools without CrewAI or LLM dependencies.
    Uses actual tool classes directly.
    """
    
    def __init__(self, output_dir="outputs"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        print("\n📦 Initializing Agent 1 tools...")
        print("   ✓ Data Profiling Tool")
        print("   ✓ Rule Generator Tool")
        
    def execute_schema_analysis(self, file_path):
        """Step 1: Generate schema context from Excel file"""
        print("\n" + "="*80)
        print("STEP 1: Schema Analysis")
        print("="*80)
        
        output_path = self.output_dir / "schema_context.json"
        
        try:
            import pandas as pd
            
            # Read Excel and extract schema information
            xl = pd.ExcelFile(file_path)
            schema_data = {
                "file_name": Path(file_path).name,
                "sheets": {}
            }
            
            for sheet_name in xl.sheet_names:
                df = xl.parse(sheet_name)
                schema_data["sheets"][sheet_name] = {
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "columns": {
                        col: {
                            "data_type": str(df[col].dtype),
                            "sample_values": df[col].dropna().head(3).tolist()
                        }
                        for col in df.columns
                    }
                }
            
            # Save schema
            with open(output_path, 'w') as f:
                json.dump(schema_data, f, indent=2)
            
            print(f"✅ Schema analysis complete: {output_path}")
            print(f"   Analyzed {len(schema_data['sheets'])} sheets")
            return str(output_path)
            
        except Exception as e:
            print(f"❌ Schema analysis failed: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def execute_profiling(self, file_path):
        """Step 2: Execute data profiling tool"""
        print("\n" + "="*80)
        print("STEP 2: Data Profiling")
        print("="*80)
        
        output_path = self.output_dir / "profiling_results.json"
        
        try:
            # Create profiling tool instance
            profiler = DataProfilingTool(file_path)
            
            # Run profiling and save results
            profiler.save_to_json(str(output_path))
            
            print(f"✅ Data profiling complete: {output_path}")
            return str(output_path)
            
        except Exception as e:
            print(f"❌ Data profiling failed: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def execute_rule_generation(self, schema_path, profiling_path):
        """Step 3: Execute rule generator tool"""
        print("\n" + "="*80)
        print("STEP 3: Rule Generation")
        print("="*80)
        
        output_path = self.output_dir / "quality_validation_instructions.yaml"
        
        try:
            # Create rule generator instance
            rule_generator = RuleGeneratorTool()
            
            # Run rule generation
            result = rule_generator._run(
                profiling_path=profiling_path,
                schema_path=schema_path,
                output_path=str(output_path)
            )
            
            # Verify the file was created
            if not output_path.exists():
                raise FileNotFoundError(f"Rule generation did not create {output_path}")
            
            print(f"✅ Rule generation complete: {output_path}")
            
            # Load and verify the rules
            import yaml
            with open(output_path, 'r') as f:
                rules = yaml.safe_load(f)
            
            entities = rules.get('entities', {})
            total_rules = sum(len(entity.get('rules', [])) for entity in entities.values())
            
            print(f"   Generated rules for {len(entities)} entities")
            print(f"   Total rules: {total_rules}")
            
            # Show rules per entity
            for entity_name, entity_data in entities.items():
                rule_count = len(entity_data.get('rules', []))
                print(f"      • {entity_name}: {rule_count} rules")
            
            return str(output_path)
            
        except Exception as e:
            print(f"❌ Rule generation failed: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def run(self, file_path):
        """Execute complete DQ discovery pipeline"""
        print("="*80)
        print("AGENT 1: Generalized DQ Discovery Expert (NO LLM)")
        print("="*80)
        print(f"Input file: {file_path}")
        print(f"Output directory: {self.output_dir}")
        
        # Verify input file exists
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")
        
        try:
            # Step 1: Schema Analysis
            schema_path = self.execute_schema_analysis(file_path)
            
            # Step 2: Data Profiling
            profiling_path = self.execute_profiling(file_path)
            
            # Step 3: Rule Generation
            rule_path = self.execute_rule_generation(schema_path, profiling_path)
            
            # Summary
            print("\n" + "="*80)
            print("✅ AGENT 1 EXECUTION COMPLETE (LLM-FREE)")
            print("="*80)
            print(f"All outputs generated in '{self.output_dir}/':")
            print(f"  1. ✓ schema_context.json")
            print(f"  2. ✓ profiling_results.json")
            print(f"  3. ✓ quality_validation_instructions.yaml")
            print("="*80)
            
            return {
                "status": "success",
                "outputs": {
                    "schema": schema_path,
                    "profiling": profiling_path,
                    "rules": rule_path
                }
            }
            
        except Exception as e:
            print("\n" + "="*80)
            print("❌ AGENT 1 EXECUTION FAILED")
            print("="*80)
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            raise


def run_agent_1(file_path, output_dir="outputs"):
    """
    Main entry point for Agent 1 - completely LLM-free execution
    """
    # Clean environment (just in case)
    os.environ.pop("OPENAI_API_KEY", None)
    os.environ.pop("CREWAI_DEFAULT_LLM_PROVIDER", None)
    
    # Create and run agent
    agent = LLMFreeDQAgent(output_dir=output_dir)
    result = agent.run(file_path)
    
    return result


if __name__ == "__main__":
    # ✅ UPDATE THIS PATH to your actual Excel file
    file_path = r"D:\Muskan.Verma_OneDrive_Data\OneDrive - Course5 Intelligence Limited\Desktop\dqm_agentic_system\data\DQ_dataset.xlsx"  # ✅ CORRECT!
    print("RUNNING AGENT 1: DQ DISCOVERY EXPERT (LLM-FREE)")
    print("="*80)
    
    try:
        result = run_agent_1(file_path, output_dir="outputs")
        print("\n✅ Success! Result:", json.dumps(result, indent=2))
    except Exception as e:
        print(f"\n❌ Execution failed: {e}")
        import traceback
        traceback.print_exc()
