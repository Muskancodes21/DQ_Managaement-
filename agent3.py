"""
Agent 3: Remediation Service (Backend Only)
AI-powered data quality remediation - no Flask, just logic
Used by the main dashboard
"""

import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime
from openai import OpenAI


class RemediationService:
    """
    Backend service for AI-powered data quality remediation
    """
    
    def __init__(self, output_dir, dataset_path, use_ai=True):
        """
        Initialize Remediation Service
        
        Args:
            output_dir: Path to outputs folder
            dataset_path: Path to original Excel file
            use_ai: Whether to use Groq AI
        """
        self.output_dir = Path(output_dir)
        self.dataset_path = Path(dataset_path)
        self.remediation_log_path = self.output_dir / "remediation_logs"
        self.remediation_log_path.mkdir(exist_ok=True)
        
        self.use_ai = use_ai
        self.profiling_data = None
        self.quality_issues = None
        self.suggested_actions = []
        self.executed_actions = []
        
        # Initialize Groq AI client
        if self.use_ai:
            try:
                self.groq_client = OpenAI(
                    api_key=os.environ.get("GROQ_API_KEY"),
                    base_url="https://api.groq.com/openai/v1",
                )
                self.model = "llama-3.3-70b-versatile"
                print("✓ Groq AI enabled for remediation suggestions")
            except Exception as e:
                print(f"⚠ Groq AI initialization failed: {e}")
                print("  Falling back to rule-based suggestions")
                self.use_ai = False
    
    def load_data(self):
        """Load profiling and issues data"""
        try:
            # Load profiling results
            profiling_file = self.output_dir / "profiling_results.json"
            if not profiling_file.exists():
                print(f"❌ Profiling file not found: {profiling_file}")
                return False
            
            with open(profiling_file, 'r') as f:
                self.profiling_data = json.load(f)
            
            # Load quality issues
            issues_file = self.output_dir / "quality_issues_with_remediation.csv"
            if not issues_file.exists():
                print(f"❌ Issues file not found: {issues_file}")
                return False
            
            self.quality_issues = pd.read_csv(issues_file)
            
            print(f"✓ Loaded data for remediation service")
            return True
            
        except Exception as e:
            print(f"✗ Error loading data: {e}")
            return False
    
    def get_ai_suggestion(self, issue_context):
        """
        Get AI-powered remediation suggestion from Groq
        """
        if not self.use_ai:
            return None
        
        try:
            prompt = f"""You are a pharmaceutical data quality expert. Analyze this data quality issue and provide the BEST remediation recommendation.

Issue Details:
- Table/Sheet: {issue_context['table']}
- Column: {issue_context['column']}
- Issue Type: {issue_context['issue_type']}
- Issue Count: {issue_context['issue_count']}
- Severity: {issue_context['severity']}
- Data Type: {issue_context.get('data_type', 'Unknown')}
- Null Percentage: {issue_context.get('null_percentage', 'N/A')}%

Context:
- This is a {issue_context['table']} table in a pharmaceutical manufacturing system
- Data integrity is CRITICAL for regulatory compliance (FDA, GMP)
- Business impact must be considered
- Patient safety may be affected

Provide your response in this exact JSON format:
{{
    "recommended_action": "specific action to take",
    "reasoning": "why this is the best approach for pharma data (2-3 sentences)",
    "business_impact": "potential business/regulatory consequences (1-2 sentences)",
    "alternative": "one alternative approach if the recommended fails"
}}

Be specific, practical, and regulatory-focused. Return ONLY valid JSON."""

            response = self.groq_client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=500
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON
            if '```json' in result_text:
                result_text = result_text.split('```json')[1].split('```')[0].strip()
            elif '```' in result_text:
                result_text = result_text.split('```')[1].split('```')[0].strip()
            
            ai_suggestion = json.loads(result_text)
            return ai_suggestion
            
        except Exception as e:
            print(f"⚠ AI suggestion failed: {e}")
            return None
    
    def _get_base_suggestions(self, rule_type, issue_count, data_type):
        """Get rule-based suggestions (fallback)"""
        
        rule_lower = rule_type.lower()
        
        # Null value issues
        if 'null' in rule_lower or 'missing' in rule_lower:
            if 'numeric' in data_type.lower() or 'int' in data_type.lower() or 'float' in data_type.lower():
                return {
                    'action': 'Fill with Median',
                    'description': f'Fill {issue_count} null values with column median',
                    'business_impact': 'Maintains statistical distribution',
                    'alternatives': ['Fill with Mean', 'Fill with Zero', 'Remove Rows', 'Forward Fill', 'Do Nothing']
                }
            else:
                return {
                    'action': 'Fill with Mode',
                    'description': f'Fill {issue_count} null values with most common value',
                    'business_impact': 'Uses most frequent category',
                    'alternatives': ['Fill with "Unknown"', 'Remove Rows', 'Do Nothing']
                }
        
        # Duplicate issues
        elif 'duplicate' in rule_lower:
            return {
                'action': 'Remove Duplicates (Keep First)',
                'description': f'Remove {issue_count} duplicate entries, keeping first occurrence',
                'business_impact': 'Ensures data uniqueness',
                'alternatives': ['Remove Duplicates (Keep Last)', 'Flag for Manual Review', 'Do Nothing']
            }
        
        # Range/outlier issues
        elif 'range' in rule_lower or 'outlier' in rule_lower:
            return {
                'action': 'Flag for Review',
                'description': f'Flag {issue_count} out-of-range values for business validation',
                'business_impact': 'May indicate data entry errors or exceptional cases',
                'alternatives': ['Cap at Threshold', 'Remove Outliers', 'Transform Data', 'Do Nothing']
            }
        
        # Format issues
        elif 'format' in rule_lower or 'pattern' in rule_lower:
            return {
                'action': 'Standardize Format',
                'description': f'Standardize {issue_count} values to consistent format',
                'business_impact': 'Improves data consistency',
                'alternatives': ['Remove Invalid Rows', 'Flag for Manual Review', 'Set to NULL', 'Do Nothing']
            }
        
        # Default
        else:
            return {
                'action': 'Flag for Review',
                'description': f'Manual review recommended for {issue_count} issues',
                'business_impact': 'Requires case-by-case evaluation',
                'alternatives': ['Do Nothing']
            }
    
    def generate_suggestions(self):
        """Generate AI-enhanced remediation suggestions"""
        
        if self.quality_issues is None:
            return []
        
        self.suggested_actions = []
        action_id = 1
        
        print(f"\n{'='*60}")
        print("Generating AI-Enhanced Remediation Suggestions")
        print(f"{'='*60}\n")
        
        # Group issues by entity, column, and rule
        if 'entity' not in self.quality_issues.columns:
            print("❌ 'entity' column not found in issues CSV")
            return []
        
        issue_groups = self.quality_issues.groupby(['entity', 'column', 'rule_violated'])
        
        for (entity, column, rule_type), group in issue_groups:
            issue_count = len(group)
            severity = group['severity'].iloc[0] if 'severity' in group.columns else 'Medium'
            
            # Get column profile for context
            entity_data = self.profiling_data.get('entities', {}).get(entity, {})
            col_profile = entity_data.get('columns', {}).get(column, {})
            
            data_type = col_profile.get('data_type', 'Unknown')
            null_pct = col_profile.get('null_percentage', 0)
            
            # Build context for AI
            issue_context = {
                'table': entity,
                'column': column,
                'issue_type': rule_type,
                'issue_count': issue_count,
                'severity': severity,
                'data_type': data_type,
                'null_percentage': null_pct
            }
            
            print(f"  Analyzing: {entity}.{column} ({rule_type})...")
            
            # Get AI suggestion
            ai_suggestion = self.get_ai_suggestion(issue_context)
            
            # Get base suggestions (fallback)
            base_suggestions = self._get_base_suggestions(rule_type, issue_count, data_type)
            
            # Merge AI with base suggestions
            if ai_suggestion:
                suggested_action = ai_suggestion['recommended_action']
                description = f"🤖 AI: {ai_suggestion['reasoning']}"
                business_impact = ai_suggestion.get('business_impact', '')
                alternatives = [ai_suggestion['alternative']] + base_suggestions['alternatives']
                print(f"    ✓ AI: {suggested_action}")
            else:
                suggested_action = base_suggestions['action']
                description = base_suggestions['description']
                business_impact = base_suggestions.get('business_impact', '')
                alternatives = base_suggestions['alternatives']
                print(f"    → Rule-based: {suggested_action}")
            
            self.suggested_actions.append({
                'action_id': action_id,
                'table': entity,
                'column': column,
                'issue_type': rule_type,
                'issue_count': issue_count,
                'severity': severity,
                'data_type': data_type,
                'suggested_action': suggested_action,
                'description': description,
                'business_impact': business_impact,
                'alternative_actions': alternatives[:5],
                'impact': f'Affects {issue_count} rows',
                'auto_executable': True,
                'ai_powered': ai_suggestion is not None
            })
            action_id += 1
        
        print(f"\n✓ Generated {len(self.suggested_actions)} suggestions")
        print(f"  - AI-powered: {sum(1 for a in self.suggested_actions if a.get('ai_powered', False))}")
        print(f"  - Rule-based: {sum(1 for a in self.suggested_actions if not a.get('ai_powered', False))}")
        
        return self.suggested_actions
    
    def execute_action(self, action_id, chosen_action):
        """Execute a specific remediation action"""
        
        action = next((a for a in self.suggested_actions if a['action_id'] == action_id), None)
        if not action:
            return {'status': 'error', 'message': 'Action not found'}
        
        try:
            # Load Excel file and specific sheet
            excel_file = pd.ExcelFile(self.dataset_path)
            
            # Find matching sheet
            sheet_name = None
            for name in excel_file.sheet_names:
                if name.lower() == action['table'].lower():
                    sheet_name = name
                    break
            
            if not sheet_name:
                return {'status': 'error', 'message': f'Sheet not found: {action["table"]}'}
            
            df = excel_file.parse(sheet_name)
            original_rows = len(df)
            
            # Execute based on chosen action
            if chosen_action == 'Do Nothing':
                result = {
                    'status': 'skipped',
                    'message': 'No action taken',
                    'rows_affected': 0
                }
            
            elif 'Fill with Median' in chosen_action:
                column = action['column']
                if pd.api.types.is_numeric_dtype(df[column]):
                    median_val = df[column].median()
                    null_count = df[column].isnull().sum()
                    df[column].fillna(median_val, inplace=True)
                    
                    output_file = self.remediation_log_path / f"{action['table']}_cleaned.csv"
                    df.to_csv(output_file, index=False)
                    
                    result = {
                        'status': 'success',
                        'message': f'Filled {null_count} nulls with median ({median_val:.2f})',
                        'rows_affected': null_count,
                        'output_file': str(output_file)
                    }
                else:
                    return {'status': 'error', 'message': 'Column is not numeric'}
            
            elif 'Fill with Mean' in chosen_action:
                column = action['column']
                if pd.api.types.is_numeric_dtype(df[column]):
                    mean_val = df[column].mean()
                    null_count = df[column].isnull().sum()
                    df[column].fillna(mean_val, inplace=True)
                    
                    output_file = self.remediation_log_path / f"{action['table']}_cleaned.csv"
                    df.to_csv(output_file, index=False)
                    
                    result = {
                        'status': 'success',
                        'message': f'Filled {null_count} nulls with mean ({mean_val:.2f})',
                        'rows_affected': null_count,
                        'output_file': str(output_file)
                    }
                else:
                    return {'status': 'error', 'message': 'Column is not numeric'}
            
            elif 'Fill with Mode' in chosen_action or 'Fill with "Unknown"' in chosen_action:
                column = action['column']
                null_count = df[column].isnull().sum()
                
                if 'Unknown' in chosen_action:
                    fill_val = 'Unknown'
                else:
                    fill_val = df[column].mode()[0] if not df[column].mode().empty else 'Unknown'
                
                df[column].fillna(fill_val, inplace=True)
                
                output_file = self.remediation_log_path / f"{action['table']}_cleaned.csv"
                df.to_csv(output_file, index=False)
                
                result = {
                    'status': 'success',
                    'message': f'Filled {null_count} nulls with "{fill_val}"',
                    'rows_affected': null_count,
                    'output_file': str(output_file)
                }
            
            elif 'Remove Rows' in chosen_action or 'Remove Invalid Rows' in chosen_action:
                # Get row indices from quality issues
                quality_issues = self.quality_issues[
                    (self.quality_issues['entity'] == action['table']) &
                    (self.quality_issues['column'] == action['column']) &
                    (self.quality_issues['rule_violated'] == action['issue_type'])
                ]
                
                if 'row_index' in quality_issues.columns:
                    invalid_indices = quality_issues['row_index'].dropna().astype(int).tolist()
                    df = df.drop(invalid_indices, errors='ignore')
                else:
                    df = df.dropna(subset=[action['column']])
                
                output_file = self.remediation_log_path / f"{action['table']}_cleaned.csv"
                df.to_csv(output_file, index=False)
                
                result = {
                    'status': 'success',
                    'message': f'Removed {original_rows - len(df)} invalid rows',
                    'rows_affected': original_rows - len(df),
                    'output_file': str(output_file)
                }
            
            elif 'Remove Duplicates' in chosen_action:
                keep_option = 'first' if 'Keep First' in chosen_action else 'last'
                before = len(df)
                df = df.drop_duplicates(subset=[action['column']], keep=keep_option)
                
                output_file = self.remediation_log_path / f"{action['table']}_cleaned.csv"
                df.to_csv(output_file, index=False)
                
                result = {
                    'status': 'success',
                    'message': f'Removed {before - len(df)} duplicates',
                    'rows_affected': before - len(df),
                    'output_file': str(output_file)
                }
            
            else:
                result = {
                    'status': 'not_implemented',
                    'message': f'Action "{chosen_action}" not yet implemented',
                    'rows_affected': 0
                }
            
            # Log the action
            self._log_action(action, chosen_action, result)
            self.executed_actions.append({
                'action': action,
                'chosen_action': chosen_action,
                'result': result,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
            return result
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {'status': 'error', 'message': str(e)}
    
    def _log_action(self, action, chosen_action, result):
        """Log remediation action to audit trail"""
        log_entry = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'action_id': action['action_id'],
            'table': action['table'],
            'column': action['column'],
            'issue_type': action['issue_type'],
            'action_taken': chosen_action,
            'status': result['status'],
            'rows_affected': result.get('rows_affected', 0),
            'message': result.get('message', '')
        }
        
        log_file = self.remediation_log_path / "remediation_audit_log.csv"
        log_df = pd.DataFrame([log_entry])
        
        if log_file.exists():
            existing_log = pd.read_csv(log_file)
            log_df = pd.concat([existing_log, log_df], ignore_index=True)
        
        log_df.to_csv(log_file, index=False)
        print(f"✓ Logged action: {chosen_action} on {action['table']}.{action['column']}")


# Standalone test
if __name__ == "__main__":
    import os
    
    os.environ["GROQ_API_KEY"] = "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"
    
    OUTPUT_DIR = Path("outputs")
    DATASET_PATH = Path(r"D:\Muskan.Verma_OneDrive_Data\OneDrive - Course5 Intelligence Limited\Desktop\Pharma Dataset\pharmaceutical_manufacturing_with_dq_issues.xlsx")
    
    print("\n" + "="*60)
    print("TESTING REMEDIATION SERVICE")
    print("="*60)
    
    service = RemediationService(OUTPUT_DIR, DATASET_PATH, use_ai=True)
    
    if service.load_data():
        suggestions = service.generate_suggestions()
        print(f"\n✅ Generated {len(suggestions)} suggestions")
        
        if suggestions:
            print("\nFirst suggestion:")
            first = suggestions[0]
            print(f"  Action ID: {first['action_id']}")
            print(f"  Table: {first['table']}")
            print(f"  Column: {first['column']}")
            print(f"  Issue: {first['issue_type']}")
            print(f"  Suggested: {first['suggested_action']}")
            print(f"  AI-powered: {first['ai_powered']}")
    else:
        print("\n❌ Failed to load data")
