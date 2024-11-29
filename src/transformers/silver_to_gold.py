class SilverToGoldTransformer(BaseTransformer):
    """Transform silver data to gold layer with business logic and aggregations."""
    
    def transform(self, input_paths: List[Path], **kwargs) -> Path:
        """
        Transform silver data to gold format.
        
        Transformations include:
        - Joining different sources
        - Calculating business metrics
        - Creating aggregations
        - Applying business rules
        """
        self.logger.info("Starting silver to gold transformation")
        
        # Read and combine silver datasets
        dfs = []
        for path in input_paths:
            df = pd.read_parquet(path)
            source = path.stem.split('_')[0]
            df['data_source'] = source
            dfs.append(df)
        
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Apply transformations
        transformed_df = (combined_df
                        .pipe(self._calculate_metrics)
                        .pipe(self._create_aggregations)
                        .pipe(self._apply_business_rules))
        
        # Save to gold layer
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = self.gold_path / f"economic_indicators_gold_{timestamp}.parquet"
        
        transformed_df.to_parquet(output_path, index=False)
        
        self.logger.info(f"Completed silver to gold transformation: {output_path}")
        return output_path
    
    def _calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate business metrics."""
        # Calculate year-over-year changes
        df['yoy_change'] = df.groupby(['country', 'indicator'])['value'].pct_change(periods=4)
        
        # Calculate moving averages
        df['ma_3year'] = df.groupby(['country', 'indicator'])['value'].rolling(window=12).mean().reset_index(0, drop=True)
        
        # Calculate z-scores for anomaly detection
        df['zscore'] = df.groupby(['country', 'indicator'])['value'].transform(lambda x: (x - x.mean()) / x.std())
        
        return df
    
    def _create_aggregations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create relevant aggregations."""
        # Create country-level aggregations
        country_aggs = df.groupby(['country', 'year']).agg({
            'value': ['mean', 'std', 'min', 'max'],
            'yoy_change': 'mean',
            'zscore': 'mean'
        }).reset_index()
        
        # Create global aggregations
        global_aggs = df.groupby(['year', 'indicator']).agg({
            'value': ['mean', 'median', 'std']
        }).reset_index()
        
        return df
    
    def _apply_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business rules and categorizations."""
        # Categorize growth rates
        df['growth_category'] = pd.cut(df['yoy_change'],
                                     bins=[-np.inf, -0.02, 0.02, np.inf],
                                     labels=['Contraction', 'Stable', 'Growth'])
        
        # Flag anomalies
        df['is_anomaly'] = abs(df['zscore']) > 3
        
        return df