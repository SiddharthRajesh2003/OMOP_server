import pandas as pd
import numpy as np
from transformers import AutoTokenizer, AutoModel
import torch
from sklearn.metrics.pairwise import cosine_similarity
import logging
from typing import List, Dict
import re
from tqdm import tqdm
import warnings
from omop_server.utils.logging_manager import get_logger

logger=get_logger(__name__)
warnings.filterwarnings('ignore')

class TransformerConceptMapper:
    """ 
    Advanced concept mapping for OMOP concept mapping using transformer models
    """
    
    def __init__(self, 
                 model_name: str = 'sentence-transformers/all-MiniLM-L6-v2',
                 threshold: float = 0.7,
                 batch_size:int = 32,
                 device:str = None):
        """
        Initialize with transformer model and similarity threshold
        
        Args:
            model_name: HuggingFace model name for embeddings
            threshold: Minimum cosine similarity for a match (0-1)
            batch_size: Batch size for processing
            device: Device to run model on ('cuda', 'cpu', or None for auto)
        """
        
        self.model_name = model_name
        self.threshold = threshold
        self.batch_size = batch_size
        
        if device is None:
            self.device = torch.device('cuda:0' if torch.cuda.is_available() else "cpu")
        else:
            self.device = torch.device(device)
            
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.model.to(self.device)
        self.model.eval()
        
        self.logger = logger
        
        self.concept_embeddings_cache = {}
        
        self.logger.info(f'Initialized TransformerConceptMapper with {model_name} on {self.device}')
    
    def get_embeddings(self, texts: List[str], show_progress: bool = False) -> np.ndarray:
        """
        Generate embeddings for a list of texts
        
        Args:
            texts: List of text strings
            show_progress: Whether to show progress bar
            
        Returns:
            numpy array of embeddings
        """
        
        embeddings = []
        iterator = range(0, len(texts), self.batch_size)
        if show_progress:
            iterator = tqdm(iterator, desc = 'Generating Embeddings')
        
        with torch.no_grad():
            for i in iterator:
                batch_texts = texts[i:i+self.batch_size]
                
                encoded = self.tokenizer(
                    batch_texts,
                    padding=True,
                    truncation=True,
                    return_tensors = 'pt',
                    max_length = 512
                )
                
                encoded = {k: v.to(self.device) for k, v in encoded.items()}
                outputs = self.model(**encoded)
                
                attention_mask = encoded['attention_mask']
                token_embeddings = outputs.last_hidden_state
                input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size())
                batch_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)
                
                embeddings.extend(batch_embeddings.cpu().numpy())
        
        return np.array(embeddings)
    
    def preprocess_text(self, text:str) -> str:
        """
        Enhanced text preprocessing for healthcare terms
        """
        if pd.isna(text) or text == '':
            return ''
        
        text = str(text).lower()
        
        # Healthcare-specific abbreviations and synonyms
        medical_abbreviations = {
            # Visit types
            'ed': 'emergency department',
            'er': 'emergency room',
            'emerg': 'emergency',
            'dept': 'department',
            'op': 'outpatient',
            'ip': 'inpatient',
            'icu': 'intensive care unit',
            'or': 'operating room',
            'pacu': 'post anesthesia care unit',
            'ccu': 'cardiac care unit',
            'nicu': 'neonatal intensive care unit',
            'picu': 'pediatric intensive care unit',
            
            # Procedures
            'surg': 'surgery',
            'proc': 'procedure',
            'dx': 'diagnosis',
            'tx': 'treatment',
            'rx': 'prescription',
            'lab': 'laboratory',
            'rad': 'radiology',
            'path': 'pathology',
            
            # Common medical terms
            'pt': 'patient',
            'hx': 'history',
            'f/u': 'follow up',
            'followup': 'follow up',
            'consult': 'consultation',
            'eval': 'evaluation',
            'assess': 'assessment',
            'mgmt': 'management',
            'admin': 'administration',
            'monitoring': 'monitor'
        }
        
        for abbr, full in medical_abbreviations.items():
            text = re.sub(r'\b' + re.escape(abbr) + r'\b', full, text)
        
        # Remove extra whitespace and special characters (but keep medical meaningful ones)
        text = re.sub(r'[^\w\s\-/]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def semantic_match_concepts(
        self, source_df: pd.DataFrame, concept_df: pd.DataFrame,
        source_column: str, concept_column_name: str = 'concept_name',
        return_scores: bool = False, preprocess: bool = True
    ) -> pd.DataFrame:
        """
        Perform semantic matching using transformer embeddings
        
        Args:
            source_df: DataFrame with source data
            concept_df: DataFrame with OMOP concepts
            source_column: Column name in source_df to match
            concept_name_col: Column name in concept_df for concept names
            return_scores: Whether to return similarity scores
            preprocess: Whether to preprocess text
            
        Returns:
            DataFrame with matched concept_ids
        """
        self.logger.info(f'Starting semantic matching with {len(source_df)} source records and {len(concept_df)} concepts...')
        
        if preprocess:
            source_texts = source_df[source_column].apply(self.preprocess_text).to_list()
            concept_texts = concept_df[concept_column_name].apply(self.preprocess_text).to_list()
        else:
            source_texts = source_df[source_column].fillna('').astype(str).tolist()
            concept_texts = concept_df[concept_column_name].fillna('').astype(str).tolist()
        
        self.logger.info('Generating source value embeddings...')
        source_embeddings = self.get_embeddings(source_texts, show_progress=True)
        
        self.logger.info('Generating concept embeddings')
        concept_embeddings = self.get_embeddings(concept_texts, show_progress=True)
        
        self.logger.info('Calculating similarity')
        similarities = cosine_similarity(source_embeddings, concept_embeddings)
        
        result_df = source_df.copy()
        
        best_matches = np.argmax(similarities, axis=1)
        best_scores = np.max(similarities, axis=1)        
        
        valid_matches = best_scores >= self.threshold
        
        result_df['concept_id']=None
        result_df['matched_concept_name']=None
        
        for i, (match_i, score, is_valid) in enumerate(zip(best_matches, best_scores, valid_matches)):
            if is_valid:
                result_df.loc[i, 'concept_id'] = concept_df.iloc[match_i]['concept_id']
                result_df.loc[i, 'matched_concept_name'] = concept_df.iloc[match_i][concept_column_name]
        
        if return_scores:
            result_df['similarity_score'] = best_scores

        return result_df
    
    def multi_strategy_matching(
        self, source_df: pd.DataFrame, concept_df: pd.DataFrame,
        source_column: str, concept_column_name:str = 'concept_name'
    ) -> pd.DataFrame:
        """
        Advanced matching with multiple strategies and fallbacks
        
        Args:
            source_df: Dataframe with source data
            concept_df: DataFrame with OMOP concepts
            concept_column_name: Column name for concept names
            source_column: Column name in source_df to map concepts
        """
        self.logger.info('Starting multi-strategy matching...')
        
        self.logger.info('Strategy 1: Exact Matching...')
        result_df=source_df.copy()
        result_df['concept_id'] = None
        result_df['matched_concept_name'] = None
        result_df['matching_strategy'] = None
        result_df['similarity_score'] = 0.0
        
        source_processed = source_df[source_column].apply(self.preprocess_text)
        concept_processed = concept_df[concept_column_name].apply(self.preprocess_text)
        
        concept_mapping = dict(zip(concept_processed, concept_df['concept_id']))
        concept_name_mapping = dict(zip(concept_processed, concept_df[concept_column_name]))
        
        exact_matches = 0
        for i, p_source in enumerate(source_processed):
            if p_source in concept_mapping:
                result_df.loc[i, 'concept_id'] = concept_mapping[p_source]
                result_df.loc[i, 'matched_concept_name'] = concept_name_mapping[p_source]
                result_df.loc[i, 'matching_strategy'] = 'exact_match'
                result_df.loc[i, 'similarity_score'] = 1.0
                exact_matches +=1
        
        self.logger.info(f'Exact matches found: {exact_matches}')
        
        unmatched_df = result_df[result_df['concept_id'].isna()].copy()
        
        if len(unmatched_df) > 0:
            self.logger.info(f'Strategy 2: Semantic Matching for {len(unmatched_df)} remaining unmatched records...') 
            
            unmatched_texts = unmatched_df[source_column].apply(self.preprocess_text).to_list()
            source_embeddings = self.get_embeddings(unmatched_texts, show_progress=True)
            
            concept_texts = concept_df[concept_column_name].apply(self.preprocess_text).to_list()
            concept_embeddings = self.get_embeddings(concept_texts, show_progress=True)
            
            similarities = cosine_similarity(source_embeddings, concept_embeddings)
            
            best_matches = np.argmax(similarities, axis = 1)
            best_scores = np.max(similarities, axis = 1)
            
            semantic_matches = 0
            for i, (org_i, match_i, score) in enumerate(zip(unmatched_df.index, best_matches, best_scores)):
                if score >= self.threshold:
                    result_df.loc[org_i, 'concept_id'] = concept_df.iloc[match_i]['concept_id']
                    result_df.loc[org_i, 'matched_concept_name'] = concept_df.iloc[match_i][concept_column_name]
                    result_df.loc[org_i, 'matching_strategy'] = 'semantic_match'
                    result_df.loc[org_i, 'similarity_score'] = score
                    semantic_matches+=1
            
            self.logger.info(f'Semantic matches found: {semantic_matches}')
            
        still_unmatched_df = result_df[result_df['concept_id'].isna()]
        self.logger.info(f'Strategy 3: Partial Matching for {len(still_unmatched_df)} remaining records...')
        
        if len(still_unmatched_df) > 0:
            partial_threshold = max(0.5, self.threshold - 0.2)
            
            unmatched_texts = still_unmatched_df[source_column].apply(self.preprocess_text).to_list()
            source_embeddings = self.get_embeddings(unmatched_texts, show_progress=True)
            
            concept_texts = concept_df[concept_column_name].apply(self.preprocess_text).to_list()
            concept_embeddings = self.get_embeddings(concept_texts, show_progress=True)
            
            similarities = cosine_similarity(source_embeddings, concept_embeddings)
            best_matches = np.argmax(similarities, axis=1)
            best_scores = np.max(similarities, axis=1)
            
            partial_matches = 0
            for i, (org_i, match_i, score) in enumerate(zip(still_unmatched_df.index, best_matches, best_scores)):
                if score >= partial_threshold:
                    result_df.loc[org_i, 'concept_id'] = concept_df.iloc[match_i]['concept_id']
                    result_df.loc[org_i, 'matched_concept_name'] = concept_df.iloc[match_i][concept_column_name]
                    result_df.loc[org_i, 'matching_strategy'] = 'partial_match'
                    result_df.loc[org_i, 'similarity_score'] = score
                    partial_matches += 1
            
            self.logger.info(f"Partial matches: {partial_matches}")
        
        return result_df
    
    def find_similar_concepts(
        self, query_text: str,
        concept_df: pd.DataFrame,
        concept_column_name: str = 'concept_name',
        top_k: int = 10
    ) -> pd.DataFrame:
        """
        Find top-k most similar concepts for a given query
        
        Args:
            query_text: Text to find similar concepts for
            concept_df: DataFrame with OMOP concepts
            concept_column_name: Column name for concept names
            top_k: Number of top similar concepts to return
        """
        
        processed_query = self.preprocess_text(query_text)
        
        query_embedding = self.get_embeddings([processed_query])
        
        concept_texts = concept_df[concept_column_name].apply(self.preprocess_text).to_list()
        concept_embeddings = self.get_embeddings(concept_texts)
        
        similarities = cosine_similarity(query_embedding, concept_embeddings)[0]
        
        top_indices = np.argsort(similarities)[::-1][:top_k]
        
        result_df = concept_df.iloc[top_indices].copy()
        result_df['similarity_score'] = similarities[top_indices]
        result_df['query_text'] = query_text
        
        return result_df.sort_values(by = 'similarity_score', ascending=False)
    

    def create_semantic_mapping_report(
        self, mapped_df: pd.DataFrame,
        source_column: str
    ) -> Dict:
        """
        Create detailed report of semantic matching results
        """
        
        total_records = len(mapped_df)
        mapped_records = mapped_df['concept_id'].notna().sum()
        unmapped_records = total_records - mapped_records
        
        self.logger.info(f'\n=== SEMANTIC MATCHING REPORT ===')
        self.logger.info(f'Model: {self.model_name}')
        self.logger.info(f'Threshold: {self.threshold}')
        self.logger.info(f'Total Records: {total_records}')
        self.logger.info(f'Mapped Records: {mapped_records} ({mapped_records/total_records*100:.1f}%)')
        self.logger.info(f'Unmapped records: {unmapped_records} ({unmapped_records/total_records*100:.1f}%)')
        
        if 'similarity_score' in mapped_df.columns:
            mapped_scores = mapped_df[mapped_df['concept_id'].notna()]['similarity_score']
            self.logger.info(f"Average similarity score: {mapped_scores.mean():.3f}")
            self.logger.info(f"Min similarity score: {mapped_scores.min():.3f}")
            self.logger.info(f"Max similarity score: {mapped_scores.max():.3f}")
        
        # Strategy breakdown
        if 'matching_strategy' in mapped_df.columns:
            strategy_counts = mapped_df['matching_strategy'].value_counts()
            self.logger.info(f"\nMatching Strategy Breakdown:")
            for strategy, count in strategy_counts.items():
                if pd.notna(strategy):
                    self.logger.info(f"  {strategy}: {count} records")
        
                # Show medium confidence matches (0.7 ≤ score < 0.9)
        if 'similarity_score' in mapped_df.columns:
            medium_conf = mapped_df[
                (mapped_df['similarity_score'] < 0.9) & 
                (mapped_df['similarity_score'] >= 0.7) & 
                (mapped_df['concept_id'].notna())
            ]
            self.logger.info(f"\nMedium confidence matches (0.7 ≤ score < 0.9): {len(medium_conf)} records")
            if not medium_conf.empty:
                for _, row in medium_conf.head(10).iterrows():
                    score = row.get('similarity_score', 'N/A')
                    if isinstance(score, float):
                        score = f"{score:.3f}"
                    self.logger.info(f"  '{row[source_column]}' -> '{row['matched_concept_name']}' (Score: {score})")
        
        # Show unmapped values
        unmapped_values = mapped_df[mapped_df['concept_id'].isna()][source_column].value_counts().head(10)
        if not unmapped_values.empty:
            self.logger.info(f"\nTop Unmapped Values:")
            for value, count in unmapped_values.items():
                self.logger.info(f"  '{value}': {count} occurrences")
        
        return {
            'total_records': total_records,
            'mapped_records': mapped_records,
            'unmapped_records': unmapped_records,
            'mapping_rate': mapped_records/total_records*100 if total_records > 0 else 0,
            'avg_similarity': mapped_scores.mean() if 'similarity_score' in mapped_df.columns and len(mapped_scores) > 0 else None
        }
        

def demonstrate_semantic_matching():
    """
    Demonstrate semantic matching with healthcare data
    """
    
    # Sample source data with more complex healthcare terms
    source_data = pd.DataFrame({
        'patient_id': range(1, 16),
        'visit_type': [
            'Emergency Department Visit',
            'ER - chest pain',
            'Emergency room visit',
            'Urgent care center',
            'Walk-in clinic',
            'Outpatient consultation',
            'Ambulatory care visit',
            'Day surgery',
            'Inpatient hospitalization',
            'Hospital admission',
            'ICU stay',
            'Surgical procedure',
            'Follow-up appointment',
            'Telemedicine visit',
            'Home health visit'
        ]
    })
    
    # Sample OMOP concept data
    concept_data = pd.DataFrame({
        'concept_id': [9201, 9202, 9203, 9204, 9205, 9206, 9207, 9208, 9209, 9210],
        'concept_name': [
            'Emergency Department Visit',
            'Outpatient Visit',
            'Inpatient Visit',
            'Ambulatory Surgical Center Visit',
            'Urgent Care Visit',
            'Telemedicine Visit',
            'Home Health Visit',
            'Intensive Care Unit Visit',
            'Follow-up Visit',
            'Walk-in Clinic Visit'
        ],
        'domain_id': ['Visit'] * 10,
        'vocabulary_id': ['Visit'] * 10
    })
    
    print("=== SEMANTIC MATCHING DEMONSTRATION ===")
    
    # Initialize semantic mapper
    mapper = TransformerConceptMapper(
        model_name="sentence-transformers/all-MiniLM-L6-v2",
        threshold=0.7,
        batch_size=8,
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
    )
    
    # Basic semantic matching
    print("\n1. BASIC SEMANTIC MATCHING:")
    basic_result = mapper.semantic_match_concepts(
        source_data, concept_data, 'visit_type', 
        return_scores=True
    )
    
    print(basic_result[['visit_type', 'concept_id', 'matched_concept_name', 'similarity_score']])
    
    # Multi-strategy matching
    print("\n2. MULTI-STRATEGY MATCHING:")
    advanced_result = mapper.multi_strategy_matching(
        source_data, concept_data, 'visit_type'
    )
    
    print(advanced_result[['visit_type', 'concept_id', 'matched_concept_name', 
                          'matching_strategy', 'similarity_score']])
    
    # Find similar concepts for a specific query
    print("\n3. FINDING SIMILAR CONCEPTS:")
    similar_concepts = mapper.find_similar_concepts(
        "emergency room visit", concept_data, top_k=5
    )
    print(similar_concepts[['concept_name', 'similarity_score']])
    
    # Create report
    report_stats = mapper.create_semantic_mapping_report(advanced_result, 'visit_type')
    
    return advanced_result, report_stats
               
        
'''if __name__ =='__main__':
    from omop_etl.utils.conceptbuilder import ConceptBuilder
    concept_builder = ConceptBuilder(schema='Reporting_Research', domain = 'Procedure', path_to_folder='c:/Siddharth/Siddhu/IU Health Internship/OMOP_Vocab')
    
    df = concept_builder.load_concept_data()
    
    concept_mapper = TransformerConceptMapper(device='cuda')
    
    concept_name = df['concept_name'].to_list()
    embeddings = concept_mapper.get_embeddings(concept_name)'''