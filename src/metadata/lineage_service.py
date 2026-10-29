# src/metadata/lineage_service.py
import logging
from typing import List, Dict, Tuple, Set
from collections import defaultdict, deque
from src.connections.connection_manager import ConnectionManager
from src.metadata.metadata_models import MetadataRow

logger = logging.getLogger("LineageService")

class DependencyFailedException(Exception):
    pass

class LineageService:
    def __init__(self, conn_manager: ConnectionManager):
        self.conn_manager = conn_manager
        
    def get_lineage_graph(self) -> Tuple[Dict[int, List[int]], Dict[int, List[int]], Dict[Tuple[int, int], str]]:
        """
        Retrieves the lineage graph from the database.
        Returns:
            downstream_adj: Dict[source_id, List[target_id]]
            upstream_adj: Dict[target_id, List[source_id]]
            edge_attributes: Dict[(source_id, target_id), relationship_strength]
        """
        downstream_adj = defaultdict(list)
        upstream_adj = defaultdict(list)
        edge_attributes = {}
        
        try:
            with self.conn_manager.get_metadata_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT source_table_id, target_table_id, relationship_strength
                        FROM metadata.data_lineage
                    """)
                    for src, tgt, strength in cur.fetchall():
                        downstream_adj[src].append(tgt)
                        upstream_adj[tgt].append(src)
                        edge_attributes[(src, tgt)] = strength
        except Exception as e:
            logger.error(f"Failed to retrieve lineage graph: {e}")
            
        return downstream_adj, upstream_adj, edge_attributes

    def get_downstream_impact(self, source_table_id: int) -> List[int]:
        """
        Finds all downstream table IDs using BFS.
        """
        downstream_adj, _, _ = self.get_lineage_graph()
        visited = set()
        queue = deque([source_table_id])
        
        while queue:
            curr = queue.popleft()
            for child in downstream_adj.get(curr, []):
                if child not in visited:
                    visited.add(child)
                    queue.append(child)
                    
        return list(visited)

    def build_execution_dag(self, active_tables: List[MetadataRow]) -> List[MetadataRow]:
        """
        Sorts the active tables topologically based on lineage.
        If tables have no lineage defined, they are executed in their native priority order,
        but always after their explicit dependencies.
        """
        _, upstream_adj, _ = self.get_lineage_graph()
        
        # Map ID to MetadataRow
        table_map = {t.id: t for t in active_tables}
        active_ids = set(table_map.keys())
        
        # Build local adjacency lists for active tables only
        in_degree = {tid: 0 for tid in active_ids}
        local_downstream = defaultdict(list)
        
        for tgt in active_ids:
            for src in upstream_adj.get(tgt, []):
                if src in active_ids:
                    local_downstream[src].append(tgt)
                    in_degree[tgt] += 1
                    
        # Kahn's Algorithm for Topological Sort
        # We process nodes with in_degree 0. To maintain "priority" ordering as a fallback,
        # we can sort the 0-degree queue by metadata.priority.
        queue = [tid for tid in active_ids if in_degree[tid] == 0]
        # Sort queue by priority ascending
        queue.sort(key=lambda x: table_map[x].priority)
        
        sorted_ids = []
        
        while queue:
            # Always pop the highest priority (lowest integer value) available node
            curr = queue.pop(0) 
            sorted_ids.append(curr)
            
            for child in local_downstream.get(curr, []):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)
                    
            # Re-sort queue to maintain priority order among available nodes
            queue.sort(key=lambda x: table_map[x].priority)
            
        if len(sorted_ids) != len(active_ids):
            logger.warning("Cycle detected in Data Lineage! Some tables will not be executed.")
            
        return [table_map[tid] for tid in sorted_ids]
        
    def check_dependencies_status(self, table_id: int, execution_statuses: Dict[int, str]) -> str:
        """
        Evaluates if the table can run based on its parents' statuses.
        Returns:
            "READY" if all mandatory parents succeeded.
            "DEPENDENCY_FAILED" if any mandatory parent failed/skipped.
        """
        _, upstream_adj, edge_attributes = self.get_lineage_graph()
        
        for parent_id in upstream_adj.get(table_id, []):
            strength = edge_attributes.get((parent_id, table_id), "MANDATORY")
            status = execution_statuses.get(parent_id, "SUCCESS") # If parent not in this run, assume SUCCESS
            
            if status != "SUCCESS" and strength == "MANDATORY":
                return "DEPENDENCY_FAILED"
                
        return "READY"
