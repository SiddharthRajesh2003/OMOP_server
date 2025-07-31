import pandas as pd
import logging
from sqlalchemy import text
from fuzzywuzzy import fuzz, process
from .transformerconceptmapper import TransformerConceptMapper
import torch
from .logging_manager import get_logger

logger=get_logger(__name__)

class ConceptMapper:
    """
    Map source values to OMOP concept_ids based on the specified domain using the Concept and Concept_Relationship tables
    """
    
    def __init__(self, engine, domain, schema, concept_table_name):
        self.engine = engine
        self.domain = domain
        self.schema = schema
        self.table_name = concept_table_name
        self.logger = logger

    def get_concept_lookups(self) -> pd.DataFrame:
        """
        Retrieve all related domain-related (Procedure, Condition) conepts
        from the Concept table.
        Returns:
            DataFrame: Domain related Concept records
        """
        domain = self.domain
        sql = f"""
        SELECT c.concept_id, c.concept_name, c.domain_id, c.vocabulary_id, c.concept_class_id,
            c.standard_concept, c.concept_code, c.valid_start_date, c.valid_end_date, c.invalid_reason
            FROM [{self.schema}].[{self.table_name}] c
            """
        
        if domain and domain.upper() != 'ALL':
            sql += f" WHERE UPPER(c.domain_id) = '{domain.upper()}'"
        sql += " ORDER BY c.concept_name"
        
        try:
            
            df = pd.read_sql(text(sql), self.engine)
            self.logger.info(f"Retrieved {len(df)} concepts related to Domain {self.domain}")
            return df
        except Exception as e:
            self.logger.error(f'Error retrieving records: {e}')
            raise
    
    def map_source_to_concept_fuzzy(self, source_df, concept_df, source_column, 
                              mapping_strategy = 'exact_match', concept_name_col='concept_name',
                              concept_id_col = 'concept_id'):
        """
        Map source values to OMOP concept_ids using various strategies.
        
        Args:
            source_df: DataFrame containing source data
            concept_df: DataFrame containing concept lookup data
            source_column: Column name in source_df to map
            mapping_strategy: Strategy for mapping ('exact_match', 'code_match', 'fuzzy_match')
            concept_name_col: Column name in concept_df for concept names
            
        Returns:
            DataFrame: Source data with mapped concept_ids
        """
        result_df = source_df.copy()
        
        if mapping_strategy == 'exact_match':
            mapping_dict = dict(zip(
                concept_df[concept_name_col].str.upper(),
                concept_df[concept_id_col]
            ))
            result_df['concept_id'] = result_df[source_column].str.upper().map(mapping_dict)
        
        elif mapping_strategy == 'fuzzy_match':
            try:
                concept_names = concept_df[concept_name_col].tolist()
                concept_mapping = dict(zip(
                    concept_df[concept_name_col], concept_df[concept_id_col]
                ))
                def fuzzy_map(source_val, threshold = 80):
                    if pd.isna(source_val):
                        return None
                    match = process.extractOne(str(source_val), concept_names, scorer = fuzz.ratio)
                    if match and match[1] >= threshold:
                        return concept_mapping[match[0]]
                    return None
                
                result_df['concept_id'] = result_df[source_column].apply(fuzzy_map)
            
            except ImportError:
                logging.warning(f'fuzzywuzzy library not available. Using exact match instead.')
                return self.map_source_to_concept_fuzzy(source_df, concept_df, source_column, 'exact_match')
        
        # Add mapping statistics
        total_records = len(result_df)
        mapped_records = result_df['concept_id'].notna().sum()
        unmapped_records = total_records - mapped_records
        
        logging.info(f"Mapping Statistics:")
        logging.info(f"  Total records: {total_records}")
        logging.info(f"  Mapped records: {mapped_records} ({mapped_records/total_records*100:.1f}%)")
        logging.info(f"  Unmapped records: {unmapped_records} ({unmapped_records/total_records*100:.1f}%)")
        
        return result_df
    
    def map_source_to_concept_semantic(self, source_df: pd.DataFrame, concept_df: pd.DataFrame,
                                       source_column: str, concept_column_name: str = 'concept_name',
                                       model_name: str = 'sentence-transformers/all-MiniLM-L6-v2',
                                       threshold: float = 0.7, batch_size: int = 32,
                                       strategy: str = 'multi_strategy', return_scores: bool = True) -> pd.DataFrame:
        """ 
        Map source values to OMOP concept_ids using semantic similarity with transformer models.
        
        Args:
            source_df (pd.DataFrame): DataFrame containing source data
            concept_df (pd.DataFrame): DataFrame containing concept lookup data
            source_column (str): Column name in source_df to map
            concept_column_name (str): Column name in concept_df for concept names
            model_name (str): HuggingFace model name for embeddings
            threshold (float): Minimum cosine similarity for a match (0-1)
            batch_size (int): Batch size for processing
            strategy (str): Matching strategy ('basic', 'multi_strategy')
            return_scores (bool): Whether to return similarity scores
            
        Returns:
            pd.DataFrame: Source data with mapped concept_ids and additional matching info
        """
        try:
            mapper = TransformerConceptMapper(model_name=model_name, threshold=threshold, batch_size=batch_size, device='cuda' if torch.cuda.is_available() else 'cpu')
            self.logger.info(f'Starting semantic mapping with {strategy} strategy...')
            
            if strategy.lower() == 'multi_strategy':
                result_df = mapper.multi_strategy_matching(
                    source_df = source_df, concept_df = concept_df,
                    source_column=source_column, concept_column_name=concept_column_name
                )
            else:
                result_df = mapper.semantic_match_concepts(
                    source_df = source_df, concept_df = concept_df,
                    source_column=source_column, concept_column_name=concept_column_name,
                    return_scores=return_scores, preprocess=True
                )
            
            report_stats = mapper.create_semantic_mapping_report(result_df, source_column)
            
            total_records = len(result_df)
            mapped_records = result_df['concept_id'].notna().sum()
            unmapped_records = total_records - mapped_records
            
            self.logger.info(f"Semantic Mapping Statistics:")
            self.logger.info(f"  Total records: {total_records}")
            self.logger.info(f"  Mapped records: {mapped_records} ({mapped_records/total_records*100:.1f}%)")
            self.logger.info(f"  Unmapped records: {unmapped_records} ({unmapped_records/total_records*100:.1f}%)")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f'Error in semantic mapping: {e}')
            self.logger.info("Falling back to fuzzy matching...")
            # Fallback to fuzzy matching if semantic mapping fails
            return self.map_source_to_concept_fuzzy(
                source_df, concept_df, source_column, 
                'fuzzy_match', concept_column_name
            )
    
    def find_similar_concepts_for_term(self, query_text: str, concept_df: pd.DataFrame = None,
                                       concept_column_name: str = 'concept_name', top_k: int = 10,
                                       model_name: str = 'sentence-transformers/all-MiniLM-L6-v2') -> pd.DataFrame:
        """
        Find top-k most similar concepts for a given query term.
        
        Args:
            query_text (str): Text to find similar concepts for
            concept_df (pd.DataFrame): DataFrame with OMOP concepts (if None, will fetch from DB)
            concept_column_name (str): Column name for concept names
            top_k (int): Number of top similar concepts to return
            model_name (str): HuggingFace model name for embeddings
            
        Returns:
            pd.DataFrame: Top similar concepts with similarity scores
        """
        
        try:
            # Get concept data if not provided
            if concept_df is None:
                concept_df = self.get_concept_lookups()
            
            # Initialize transformer mapper
            mapper = TransformerConceptMapper(
                model_name=model_name,
                threshold=0.0,  # Set to 0 to get all similarities
                device='cuda' if torch.cuda.is_available() else 'cpu'
            )
            
            # Find similar concepts
            similar_concepts = mapper.find_similar_concepts(
                query_text=query_text,
                concept_df=concept_df,
                concept_column_name=concept_column_name,
                top_k=top_k
            )
            
            self.logger.info(f"Found {len(similar_concepts)} similar concepts for '{query_text}'")
            
            return similar_concepts
            
        except Exception as e:
            self.logger.error(f'Error finding similar concepts: {e}')
            return pd.DataFrame()
    
    def map_source_to_concept(self, source_df: pd.DataFrame, source_column: str,
                             mapping_strategy: str = 'semantic', concept_df: pd.DataFrame = None,
                             model_name: str = 'sentence-transformers/all-MiniLM-L6-v2',**kwargs) -> pd.DataFrame:
        """
        Unified method to map source values to OMOP concept_ids using different strategies.
        
        Args:
            source_df (pd.DataFrame): DataFrame containing source data
            source_column (str): Column name in source_df to map
            mapping_strategy (str): Strategy ('exact_match', 'fuzzy_match', 'semantic', 'multi_strategy')
            concept_df (pd.DataFrame): DataFrame containing concept lookup data (if None, will fetch from DB)
            model_name(str): Any specific transformers model for performing the mapping (if None, will all sentence-transformers/all-MiniLM-L6-v2)
            **kwargs: Additional parameters for specific mapping strategies
            
        Returns:
            pd.DataFrame: Source data with mapped concept_ids
        """
        
        # Get concept data if not provided
        if concept_df is None:
            concept_df = self.get_concept_lookups()
        
        if mapping_strategy in ['semantic', 'multi_strategy']:
            return self.map_source_to_concept_semantic(
                source_df=source_df,
                concept_df=concept_df,
                source_column=source_column,
                strategy=mapping_strategy,
                model_name=model_name,
                **kwargs
            )
        else:
            return self.map_source_to_concept_fuzzy(
                source_df=source_df,
                concept_df=concept_df,
                source_column=source_column,
                mapping_strategy=mapping_strategy,
                **kwargs
            )


def demonstrate_icd_to_omop_mapping():
    """
    Demonstrate mapping ICD codes to OMOP concept_ids with fallback to semantic mapping.
    Handles mixed concept types including conditions, procedures, and specialties.
    """
    import pandas as pd
    import logging
    
    # Sample source data with mixed ICD codes and descriptions
    source_data = pd.DataFrame({
        'patient_id': range(1, 16),
        'source_code': [
            'K35.9',            # ICD code for Acute appendicitis
            'K80.20',           # ICD code for Gallbladder stones
            'I25.10',           # ICD code for Coronary artery disease
            'J44.1',            # ICD code for COPD exacerbation
            'E11.9',            # ICD code for Type 2 diabetes
            'PEDS_CARDIO',      # Custom code for Pediatric Cardiology
            'Z51.11',           # ICD code for Chemotherapy
            'N18.6',            # ICD code for End stage renal disease
            'F32.9',            # ICD code for Depression
            'HEART_SURG',       # Custom code for Heart surgery
            'K35.8',            # ICD code variant
            'ORTHO_SPEC',       # Custom code for Orthopedic specialty
            'I21.9',            # ICD code for Heart attack
            'J45.9',            # ICD code for Asthma
            'ENDO_SPEC'         # Custom code for Endocrinology
        ],
        'description': [
            'Acute appendicitis',
            'Gallbladder stones',
            'Coronary artery disease',
            'COPD exacerbation',
            'Type 2 diabetes',
            'Pediatric Cardiology',
            'Chemotherapy treatment',
            'Kidney failure',
            'Depression',
            'Cardiovascular surgery',
            'Complicated appendicitis',
            'Orthopedic specialty',
            'Heart attack',
            'Asthma',
            'Endocrinology specialty'
        ],
        'source_type': [
            'ICD', 'ICD', 'ICD', 'ICD', 'ICD', 'SPECIALTY', 'ICD', 'ICD', 'ICD', 
            'PROCEDURE', 'ICD', 'SPECIALTY', 'ICD', 'ICD', 'SPECIALTY'
        ]
    })
    
    # Sample OMOP concept data with mixed domains (like your example)
    concept_data = pd.DataFrame({
        'concept_id': [
            4023928,   # Acute appendicitis
            4134287,   # Cholelithiasis  
            4170143,   # Coronary arteriosclerosis
            4145356,   # Chronic obstructive lung disease
            4193704,   # Type 2 diabetes mellitus
            45756805,  # Pediatric Cardiology (your example)
            4273629,   # Chemotherapy
            4030518,   # End-stage renal disease
            4152280,   # Depressive disorder
            4110056,   # Cardiovascular surgical procedure
            4023929,   # Complicated acute appendicitis
            45756743,  # Orthopedic Surgery
            4329847,   # Myocardial infarction
            4051466,   # Asthma
            45756744   # Endocrinology
        ],
        'concept_name': [
            'Acute appendicitis',
            'Cholelithiasis',
            'Coronary arteriosclerosis', 
            'Chronic obstructive lung disease',
            'Type 2 diabetes mellitus',
            'Pediatric Cardiology',  # Your example
            'Antineoplastic chemotherapy',
            'End-stage renal disease',
            'Depressive disorder',
            'Cardiovascular surgical procedure',
            'Complicated acute appendicitis',
            'Orthopedic Surgery',
            'Myocardial infarction',
            'Asthma',
            'Endocrinology'
        ],
        'concept_code': [
            'K35.9', 'K80.20', 'I25.10', 'J44.1', 'E11.9', 
            'OMOP4821938',  # Your example - no ICD equivalent
            'Z51.11', 'N18.6', 'F32.9', '4110056', 
            'K35.3', 'OMOP4821940', 'I21.9', 'J45.9', 'OMOP4821941'
        ],
        'domain_id': [
            'Condition', 'Condition', 'Condition', 'Condition', 'Condition',
            'Provider',  # Your example domain
            'Condition', 'Condition', 'Condition', 'Procedure',
            'Condition', 'Provider', 'Condition', 'Condition', 'Provider'
        ],
        'vocabulary_id': [
            'ICD10CM', 'ICD10CM', 'ICD10CM', 'ICD10CM', 'ICD10CM',
            'ABMS',  # Your example vocabulary
            'ICD10CM', 'ICD10CM', 'ICD10CM', 'SNOMED',
            'ICD10CM', 'ABMS', 'ICD10CM', 'ICD10CM', 'ABMS'
        ],
        'concept_class_id': [
            '10-digit billing code', '10-digit billing code', '10-digit billing code', 
            '10-digit billing code', '10-digit billing code',
            'Physician Specialty',  # Your example
            '10-digit billing code', '10-digit billing code', '10-digit billing code', 
            'Procedure', '10-digit billing code', 'Physician Specialty', 
            '10-digit billing code', '10-digit billing code', 'Physician Specialty'
        ],
        'standard_concept': ['S'] * 15
    })
    
    print("=== ICD TO OMOP CONCEPT MAPPING DEMONSTRATION ===")
    print(f"Source data contains {len(source_data)} records with ICD codes")
    print(f"Concept lookup contains {len(concept_data)} OMOP concepts")
    
    # Create a mock concept mapper
    class MockConceptMapper(ConceptMapper):
        def __init__(self, domain):
            self.domain = domain
            self.logger = logging.getLogger(__name__)
            logging.basicConfig(level=logging.INFO)
        
        def get_concept_lookups(self):
            return concept_data
    
    # Initialize mapper
    mapper = MockConceptMapper(domain='Condition')
    
    # Step 1: Try exact code matching first (ICD codes and custom codes)
    print("\n--- STEP 1: Direct Code Matching ---")
    
    # Create mapping dictionary for all concept codes (not just ICD)
    concept_code_mapping = dict(zip(concept_data['concept_code'], concept_data['concept_id']))
    concept_name_mapping = dict(zip(concept_data['concept_code'], concept_data['concept_name']))
    concept_domain_mapping = dict(zip(concept_data['concept_code'], concept_data['domain_id']))
    
    # Apply direct code mapping
    result_df = source_data.copy()
    result_df['concept_id'] = result_df['source_code'].map(concept_code_mapping)
    result_df['matched_concept_name'] = result_df['source_code'].map(concept_name_mapping)
    result_df['matched_domain'] = result_df['source_code'].map(concept_domain_mapping)
    result_df['mapping_method'] = result_df['concept_id'].apply(lambda x: 'DIRECT_CODE_MATCH' if pd.notna(x) else 'UNMAPPED')
    
    # Separate ICD matches from other direct matches
    icd_matches = result_df[(result_df['mapping_method'] == 'DIRECT_CODE_MATCH') & (result_df['source_type'] == 'ICD')]
    other_direct_matches = result_df[(result_df['mapping_method'] == 'DIRECT_CODE_MATCH') & (result_df['source_type'] != 'ICD')]
    unmapped_after_direct = result_df[result_df['mapping_method'] == 'UNMAPPED']
    
    print(f"Direct code matches: {len(result_df[result_df['mapping_method'] == 'DIRECT_CODE_MATCH'])}")
    print(f"  - ICD code matches: {len(icd_matches)}")
    print(f"  - Other direct matches: {len(other_direct_matches)}")
    print(f"Unmapped after direct matching: {len(unmapped_after_direct)}")
    
    if len(icd_matches) > 0:
        print(f"\nICD Code Matches:")
        print(icd_matches[['source_code', 'description', 'concept_id', 'matched_concept_name', 'matched_domain']].to_string(index=False))
    
    if len(other_direct_matches) > 0:
        print(f"\nOther Direct Code Matches:")
        print(other_direct_matches[['source_code', 'description', 'concept_id', 'matched_concept_name', 'matched_domain']].to_string(index=False))
    
    # Step 2: For unmapped records, try semantic mapping using descriptions
    if len(unmapped_after_direct) > 0:
        print(f"\n--- STEP 2: Semantic Mapping for {len(unmapped_after_direct)} Unmapped Records ---")
        
        try:
            # Use semantic mapping for unmapped records
            semantic_input = unmapped_after_direct[['patient_id', 'source_code', 'description', 'source_type']].copy()
            
            semantic_result = mapper.map_source_to_concept(
                source_df=semantic_input,
                source_column='description',
                mapping_strategy='semantic',
                concept_df=concept_data,
                threshold=0.6,
                return_scores=True
            )
            
            # Update mapping method for semantically mapped records
            semantic_mapped = semantic_result[semantic_result['concept_id'].notna()].copy()
            semantic_mapped['mapping_method'] = 'SEMANTIC_MATCH'
            
            # Add domain information for semantic matches
            semantic_concept_domain_mapping = dict(zip(concept_data['concept_id'], concept_data['domain_id']))
            semantic_mapped['matched_domain'] = semantic_mapped['concept_id'].map(semantic_concept_domain_mapping)
            
            semantic_unmapped = semantic_result[semantic_result['concept_id'].isna()].copy()
            semantic_unmapped['mapping_method'] = 'UNMAPPED_FINAL'
            semantic_unmapped['matched_domain'] = None
            
            print(f"Additional matches via semantic mapping: {len(semantic_mapped)}")
            print(f"Final unmapped records: {len(semantic_unmapped)}")
            
            if len(semantic_mapped) > 0:
                print(f"\nSuccessfully mapped via semantic similarity:")
                semantic_display_cols = ['source_code', 'description', 'concept_id', 'matched_concept_name', 'matched_domain']
                if 'similarity_score' in semantic_mapped.columns:
                    semantic_display_cols.append('similarity_score')
                print(semantic_mapped[semantic_display_cols].to_string(index=False))
            
            # Show cross-domain matches (important for your scenario)
            cross_domain_matches = semantic_mapped[semantic_mapped['source_type'] != semantic_mapped['matched_domain'].str.lower()]
            if len(cross_domain_matches) > 0:
                print(f"\nCross-domain matches (source type != matched domain):")
                print(cross_domain_matches[['source_code', 'source_type', 'matched_domain', 'matched_concept_name']].to_string(index=False))
            
            # Combine all results
            final_result = pd.concat([
                result_df[result_df['mapping_method'] == 'DIRECT_CODE_MATCH'],
                semantic_mapped,
                semantic_unmapped
            ], ignore_index=True)
            
        except Exception as e:
            print(f"Semantic mapping failed: {e}")
            print("Using only direct code matches...")
            final_result = result_df
    else:
        final_result = result_df
    
    # Step 3: Final Results Summary with Domain Analysis
    print(f"\n--- FINAL MAPPING RESULTS ---")
    
    total_records = len(final_result)
    direct_matches = len(final_result[final_result['mapping_method'] == 'DIRECT_CODE_MATCH'])
    semantic_matches = len(final_result[final_result['mapping_method'] == 'SEMANTIC_MATCH'])
    unmapped_final = len(final_result[final_result['mapping_method'].isin(['UNMAPPED', 'UNMAPPED_FINAL'])])
    
    print(f"Total records: {total_records}")
    print(f"Direct code matches: {direct_matches} ({direct_matches/total_records*100:.1f}%)")
    print(f"Semantic matches: {semantic_matches} ({semantic_matches/total_records*100:.1f}%)")
    print(f"Total mapped: {direct_matches + semantic_matches} ({(direct_matches + semantic_matches)/total_records*100:.1f}%)")
    print(f"Unmapped: {unmapped_final} ({unmapped_final/total_records*100:.1f}%)")
    
    # Domain analysis
    print(f"\n--- MAPPING BY DOMAIN ---")
    mapped_records = final_result[final_result['concept_id'].notna()]
    if len(mapped_records) > 0:
        domain_counts = mapped_records['matched_domain'].value_counts()
        for domain, count in domain_counts.items():
            print(f"{domain}: {count} records")
    
    # Show unmapped records for review
    if unmapped_final > 0:
        print(f"\nUnmapped records requiring manual review:")
        unmapped_records = final_result[final_result['mapping_method'].isin(['UNMAPPED', 'UNMAPPED_FINAL'])]
        print(unmapped_records[['source_code', 'description', 'source_type']].to_string(index=False))
    
    # Special handling for Provider specialty concepts (like your example)
    provider_matches = final_result[final_result['matched_domain'] == 'Provider']
    if len(provider_matches) > 0:
        print(f"\n--- PROVIDER/SPECIALTY MAPPINGS ---")
        print("Note: These are specialty concepts, not condition/procedure codes:")
        print(provider_matches[['source_code', 'description', 'concept_id', 'matched_concept_name', 'mapping_method']].to_string(index=False))
        print("Provider concepts don't typically have ICD equivalents - they represent medical specialties.")
    
    # Display final mapping method breakdown
    print(f"\n--- MAPPING METHOD BREAKDOWN ---")
    mapping_summary = final_result['mapping_method'].value_counts()
    for method, count in mapping_summary.items():
        print(f"{method}: {count} records")
    
    return final_result


# Additional helper function to analyze concept mappability
def analyze_concept_mappability():
    """
    Analyze which types of concepts can be mapped to ICD codes vs need semantic mapping
    """
    print("\n=== CONCEPT MAPPABILITY ANALYSIS ===")
    
    # Typical mappability patterns
    mappability_patterns = {
        'High ICD Mappability': {
            'domains': ['Condition', 'Observation'],
            'vocabularies': ['ICD10CM', 'ICD9CM', 'ICD10PCS'],
            'examples': ['Diabetes', 'Hypertension', 'Pneumonia', 'Appendicitis']
        },
        'Medium ICD Mappability': {
            'domains': ['Procedure'],
            'vocabularies': ['CPT4', 'HCPCS', 'ICD10PCS'],
            'examples': ['Surgery procedures', 'Diagnostic tests', 'Treatments']
        },
        'Low/No ICD Mappability': {
            'domains': ['Provider', 'Specialty', 'Device', 'Drug'],
            'vocabularies': ['ABMS', 'NUCC', 'RxNorm', 'NDC'],
            'examples': ['Pediatric Cardiology', 'Medical devices', 'Medications', 'Healthcare specialties']
        }
    }
    
    for category, info in mappability_patterns.items():
        print(f"\n{category}:")
        print(f"  Domains: {', '.join(info['domains'])}")
        print(f"  Vocabularies: {', '.join(info['vocabularies'])}")
        print(f"  Examples: {', '.join(info['examples'])}")
        
        if category == 'Low/No ICD Mappability':
            print("  → Requires semantic/description-based mapping")
            print("  → Your 'Pediatric Cardiology' example falls into this category")
    
    print(f"\nKey Insight: Provider specialties like 'Pediatric Cardiology' (concept_id: 45756805)")
    print(f"cannot be mapped via ICD codes because they represent healthcare provider")
    print(f"specialties, not medical conditions or procedures that would have ICD codes.")


if __name__ == "__main__":
    # Run the demonstration
    demonstrate_icd_to_omop_mapping()
    
    # Run the analysis
    analyze_concept_mappability()
