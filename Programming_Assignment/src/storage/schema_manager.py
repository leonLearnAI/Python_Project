"""
Schema manager for MongoDB collections
"""

import json
from pathlib import Path
from typing import Dict, Any, List
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid, OperationFailure
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SchemaManager:
    """Manages MongoDB schemas and indexes"""
    
    def __init__(self, mongo_client: MongoClient, database_name: str):
        self.client = mongo_client
        self.db = self.client[database_name]
        
    def load_schema(self, schema_file: Path) -> Dict[str, Any]:
        """Load schema from JSON file"""
        try:
            with open(schema_file, 'r') as f:
                schema = json.load(f)
            logger.info(f"Loaded schema from {schema_file}")
            return schema
        except FileNotFoundError:
            logger.error(f"Schema file not found: {schema_file}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in schema file {schema_file}: {e}")
            raise
    
    def create_collection_with_schema(self, schema: Dict[str, Any]) -> bool:
        """Create collection with schema validation"""
        collection_name = schema.get("collection")
        
        if not collection_name:
            logger.error("Schema missing collection name")
            return False
        
        try:
            # Check if collection exists
            if collection_name in self.db.list_collection_names():
                logger.info(f"Collection {collection_name} already exists, updating schema")
                
                # Update existing collection schema
                validator = schema.get("validator", {})
                validation_level = schema.get("validation_level", "strict")
                validation_action = schema.get("validation_action", "error")
                
                self.db.command({
                    "collMod": collection_name,
                    "validator": validator,
                    "validationLevel": validation_level,
                    "validationAction": validation_action
                })
                
                logger.info(f"Updated schema for collection {collection_name}")
            else:
                # Create new collection with schema
                validator = schema.get("validator", {})
                validation_level = schema.get("validation_level", "strict")
                validation_action = schema.get("validation_action", "error")
                
                self.db.create_collection(
                    collection_name,
                    validator=validator,
                    validationLevel=validation_level,
                    validationAction=validationAction
                )
                
                logger.info(f"Created collection {collection_name} with schema")
            
            return True
            
        except CollectionInvalid as e:
            logger.error(f"Collection creation failed for {collection_name}: {e}")
            return False
        except OperationFailure as e:
            logger.error(f"MongoDB operation failed for {collection_name}: {e}")
            return False
    
    def create_indexes(self, schema: Dict[str, Any]) -> bool:
        """Create indexes defined in schema"""
        collection_name = schema.get("collection")
        indexes = schema.get("indexes", [])
        
        if not collection_name or not indexes:
            logger.warning(f"No indexes defined for {collection_name}")
            return True
        
        collection = self.db[collection_name]
        
        success_count = 0
        for index_def in indexes:
            try:
                index_name = index_def.get("name", "unnamed_index")
                fields = index_def.get("fields", [])
                options = index_def.get("options", {})
                
                # Convert fields list to proper format for create_index
                index_fields = []
                for field in fields:
                    for field_name, direction in field.items():
                        index_fields.append((field_name, direction))
                
                # Create index
                collection.create_index(index_fields, **options)
                logger.info(f"Created index {index_name} for {collection_name}")
                success_count += 1
                
            except Exception as e:
                logger.error(f"Failed to create index {index_def.get('name')}: {e}")
        
        logger.info(f"Created {success_count}/{len(indexes)} indexes for {collection_name}")
        return success_count == len(indexes)
    
    def validate_document(self, collection_name: str, document: Dict[str, Any]) -> bool:
        """Validate document against collection schema"""
        try:
            # Use MongoDB's validation through insert_one (will fail if invalid)
            temp_collection = self.db[f"temp_validation_{datetime.now().timestamp()}"]
            
            # Copy schema from target collection
            coll_info = self.db[collection_name].options()
            if 'validator' in coll_info:
                temp_collection = self.db.create_collection(
                    temp_collection.name,
                    validator=coll_info['validator']
                )
            
            # Try to insert (will validate)
            result = temp_collection.insert_one(document)
            
            # Cleanup
            temp_collection.drop()
            
            return True
            
        except Exception as e:
            logger.warning(f"Document validation failed: {e}")
            return False
    
    def setup_all_schemas(self, schema_files: Dict[str, Path]) -> Dict[str, bool]:
        """Setup all schemas from files"""
        results = {}
        
        for schema_name, schema_file in schema_files.items():
            try:
                logger.info(f"Setting up schema: {schema_name}")
                
                # Load schema
                schema = self.load_schema(schema_file)
                
                # Create collection with schema
                schema_result = self.create_collection_with_schema(schema)
                
                # Create indexes
                if schema_result:
                    index_result = self.create_indexes(schema)
                    results[schema_name] = schema_result and index_result
                else:
                    results[schema_name] = False
                    
                logger.info(f"Schema setup for {schema_name}: {results[schema_name]}")
                
            except Exception as e:
                logger.error(f"Failed to setup schema {schema_name}: {e}")
                results[schema_name] = False
        
        return results
    
    def get_schema_info(self, collection_name: str) -> Dict[str, Any]:
        """Get schema information for a collection"""
        try:
            coll_info = self.db[collection_name].options()
            
            info = {
                "collection": collection_name,
                "count": self.db[collection_name].count_documents({}),
                "indexes": list(self.db[collection_name].list_indexes()),
                "validator": coll_info.get('validator', {}),
                "validation_level": coll_info.get('validationLevel', 'off'),
                "validation_action": coll_info.get('validationAction', 'warn')
            }
            
            return info
            
        except Exception as e:
            logger.error(f"Failed to get schema info for {collection_name}: {e}")
            return {}
