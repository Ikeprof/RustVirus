using System;
using System.Collections.Generic;
using System.Linq;
using Sandbox.Common.ObjectBuilders;
using Sandbox.Definitions;
using Sandbox.Game;
using Sandbox.Game.Entities;
using Sandbox.ModAPI;
using VRage.Game;
using VRage.Game.Components;
using VRage.Game.ModAPI;
using VRage.ModAPI;
using VRage.ObjectBuilders;
using VRage.Utils;
using VRageMath;

namespace Rustvirus
{
    [MySessionComponentDescriptor(MyUpdateOrder.BeforeSimulation)]
    public class RustvirusV2 : MySessionComponentBase
    {
        private Random _random = new Random();
        private const bool DEBUG_MODE = true;
        private const int UPDATE_RATE = 60; // Process queue every 60 ticks (once per second)
        private const int BLOCKS_TO_RAZE_PER_FRAME = 20; // Raze 20 blocks per update to avoid physics crashes
        private const int SPAWN_DELAY_SECONDS = 5; // Wait before processing newly spawned grids
        
        private int _updateCounter = 0;
        private Dictionary<long, GridProcessingState> _processingQueue = new Dictionary<long, GridProcessingState>();
        private Dictionary<long, DateTime> _gridSpawnTimes = new Dictionary<long, DateTime>(); // Track when grids spawn
        private bool _worldLoaded = false;

        private class GridProcessingState
        {
            public IMyCubeGrid Grid;
            public HashSet<IMySlimBlock> BlocksToKeep; // Actual block references
            public List<IMySlimBlock> BlocksToRaze; // Actual block references
            public int CurrentRazeIndex; // How many we've razed so far
        }

        public override void LoadData()
        {
            if (!MyAPIGateway.Multiplayer.IsServer)
                return;

            MyAPIGateway.Entities.OnEntityAdd += OnEntityAdded;
        }

        public override void UpdateBeforeSimulation()
        {
            if (!MyAPIGateway.Multiplayer.IsServer)
                return;

            // Wait for world to fully load before processing
            if (!_worldLoaded)
            {
                _worldLoaded = true;
                Log("World loaded, RustVirus ready");
                return;
            }

            // Check for grids that have finished their spawn delay
            var gridsToProcess = new List<IMyCubeGrid>();
            var gridsToRemove = new List<long>();
            
            foreach (var kvp in _gridSpawnTimes)
            {
                var gridId = kvp.Key;
                var spawnTime = kvp.Value;
                
                // Check if delay period has passed
                if ((DateTime.UtcNow - spawnTime).TotalSeconds >= SPAWN_DELAY_SECONDS)
                {
                    // Find the grid
                    IMyEntity entity;
                    if (MyAPIGateway.Entities.TryGetEntityById(gridId, out entity))
                    {
                        var grid = entity as IMyCubeGrid;
                        if (grid != null && !grid.Closed)
                        {
                            gridsToProcess.Add(grid);
                        }
                    }
                    gridsToRemove.Add(gridId);
                }
            }
            
            // Process grids that finished their delay
            foreach (var grid in gridsToProcess)
            {
                try
                {
                    // Skip signal grids and Factorum
                    if (IsExcludedSpecialGrid(grid))
                    {
                        if (DEBUG_MODE)
                            Log($"[DEBUG] {grid.DisplayName} excluded as special grid");
                        continue;
                    }
                    
                    // Skip player-owned grids
                    if (IsPlayerOwnedGrid(grid))
                    {
                        if (DEBUG_MODE)
                            Log($"[DEBUG] {grid.DisplayName} skipped - player owned");
                        continue;
                    }

                    // It's an NPC grid - add to processing queue
                    Log($"Queueing NPC grid for salvage: {grid.DisplayName}");
                    QueueGridForProcessing(grid);
                }
                catch (Exception ex)
                {
                    Log($"Error processing delayed grid: {ex}");
                }
            }
            
            // Clean up processed spawn times
            foreach (var gridId in gridsToRemove)
            {
                _gridSpawnTimes.Remove(gridId);
            }

            if (++_updateCounter % UPDATE_RATE != 0)
                return;

            ProcessQueue();
        }

        protected override void UnloadData()
        {
            if (!MyAPIGateway.Multiplayer.IsServer)
                return;

            MyAPIGateway.Entities.OnEntityAdd -= OnEntityAdded;
            
            // Grid split events already unsubscribed in QueueGridForProcessing
        }

        private void OnEntityAdded(IMyEntity entity)
        {
            try
            {
                // Don't process entities until world is fully loaded
                if (!_worldLoaded) return;
                
                var grid = entity as IMyCubeGrid;
                if (grid == null) return;
                
                // Track spawn time and delay processing to let MES/spawners finish setup
                _gridSpawnTimes[grid.EntityId] = DateTime.UtcNow;
                
                if (DEBUG_MODE)
                    Log($"[DEBUG] Entity added: {grid.DisplayName}, Owners: {grid.BigOwners.Count} (delaying {SPAWN_DELAY_SECONDS}s)");
            }
            catch (Exception ex)
            {
                Log($"Error in OnEntityAdded: {ex}");
            }
        }

        private void QueueGridForProcessing(IMyCubeGrid grid)
        {
            if (_processingQueue.ContainsKey(grid.EntityId))
                return; // Already queued

            // Don't subscribe to grid splits - we handle them via block references
            // grid.OnGridSplit += OnGridSplit;  // REMOVED - causes crashes

            // Apply rust immediately
            // DISABLED: Not working and may be causing physics crashes
            // ApplyRustToGrid(grid);

            // Disable power AFTER we've identified the grid (beacons help players find wrecks)
            // This happens now, 5 seconds after spawn
            DisableAllPower(grid);

            // Get all functional blocks
            var blocks = new List<IMySlimBlock>();
            grid.GetBlocks(blocks, block => !IsExcludedBlock(block) && block.FatBlock != null);

            if (blocks.Count == 0)
            {
                Log($"No functional blocks on {grid.DisplayName}");
                return;
            }

            // Find valuable blocks that haven't been processed yet (no CustomData marker)
            var valuableBlocks = blocks.Where(block => 
            {
                if (!IsValuableBlock(block)) 
                    return false;
                
                // Skip blocks with CustomData (already processed or player-modified)
                var terminalBlock = block.FatBlock as IMyTerminalBlock;
                if (terminalBlock != null && !string.IsNullOrEmpty(terminalBlock.CustomData))
                {
                    if (DEBUG_MODE)
                    {
                        string subtypeName = block.BlockDefinition?.Id.SubtypeName ?? "Unknown";
                        Log($"[DEBUG] Skipping {subtypeName} - has CustomData (already processed or player-modified)");
                    }
                    return false;
                }
                
                return true;
            }).ToList();
            
            if (DEBUG_MODE)
                Log($"[DEBUG] {grid.DisplayName}: {blocks.Count} total FBs, {valuableBlocks.Count} valuable");

            // Pick 1-5 random valuable blocks to keep (10 for small grids - they're harder to find)
            // Always prioritize keeping at least one power source
            int blocksToKeep = grid.GridSizeEnum == MyCubeSize.Small ? 
                Math.Min(10, valuableBlocks.Count) : 
                Math.Min(5, valuableBlocks.Count);
            var keptBlocks = new HashSet<IMySlimBlock>();
            
            if (valuableBlocks.Count > 0)
            {
                // First, ensure at least one power block is kept
                var powerBlocks = valuableBlocks.Where(b => b.FatBlock is IMyPowerProducer).ToList();
                if (powerBlocks.Count > 0)
                {
                    var powerBlock = powerBlocks[_random.Next(powerBlocks.Count)];
                    keptBlocks.Add(powerBlock);
                    
                    if (DEBUG_MODE)
                    {
                        string subtypeName = powerBlock.BlockDefinition?.Id.SubtypeName ?? "Unknown";
                        Log($"[DEBUG] Priority power block: {subtypeName}");
                    }
                }
                
                // Then fill remaining slots with random valuable blocks
                var remainingSlots = blocksToKeep - keptBlocks.Count;
                if (remainingSlots > 0)
                {
                    var availableBlocks = valuableBlocks.Where(b => !keptBlocks.Contains(b)).ToList();
                    var shuffled = availableBlocks.OrderBy(x => _random.Next()).Take(remainingSlots);
                    
                    foreach (var block in shuffled)
                    {
                        keptBlocks.Add(block);
                    }
                }
                
                // Damage all kept blocks to 20-70%
                foreach (var block in keptBlocks)
                {
                    float targetIntegrity = 0.2f + ((float)_random.NextDouble() * 0.5f);
                    float currentIntegrity = block.BuildLevelRatio;
                    float damageNeeded = (currentIntegrity - targetIntegrity) * block.MaxIntegrity;
                    
                    if (DEBUG_MODE)
                    {
                        string subtypeName = block.BlockDefinition?.Id.SubtypeName ?? "Unknown";
                        Log($"[DEBUG] Keeping {subtypeName} at target {targetIntegrity*100:F0}%");
                    }
                    
                    if (damageNeeded > 0)
                    {
                        // OLD METHOD (iteration 55) - worked but broke hacking/ownership transfer:
                        // block.DoDamage(damageNeeded, MyStringHash.GetOrCompute("Grind"), true);
                        
                        // NEW METHOD - trying hitInfo + shouldDetonateAmmo based on "Damaged Spawnships" mod
                        var hitInfo = new MyHitInfo();
                        hitInfo.Position = block.Position;
                        hitInfo.Normal = Vector3.Up;
                        hitInfo.Velocity = Vector3.Zero;
                        
                        block.DoDamage(
                            damageNeeded, 
                            MyStringHash.GetOrCompute("Decay"), 
                            sync: true, 
                            hitInfo: hitInfo,
                            attackerId: 0L,
                            shouldDetonateAmmo: false
                        );
                    }
                    
                    // Mark this block as processed with invisible marker
                    var terminalBlock = block.FatBlock as IMyTerminalBlock;
                    if (terminalBlock != null)
                    {
                        terminalBlock.CustomData = "\u200B"; // Zero-width space (invisible to players)
                    }
                }
            }

            // Build list of blocks to raze (store actual blocks, not positions)
            var blocksToRaze = new List<IMySlimBlock>();
            foreach (var block in blocks)
            {
                if (!keptBlocks.Contains(block))
                {
                    // Also don't raze blocks with CustomData (already processed or player-modified)
                    var terminalBlock = block.FatBlock as IMyTerminalBlock;
                    if (terminalBlock != null && !string.IsNullOrEmpty(terminalBlock.CustomData))
                    {
                        if (DEBUG_MODE)
                        {
                            string subtypeName = block.BlockDefinition?.Id.SubtypeName ?? "Unknown";
                            Log($"[DEBUG] Not razing {subtypeName} - has CustomData");
                        }
                        continue; // Don't raze marked blocks
                    }
                    
                    // Special case: Small grid O2/H2 generators - 50% chance to keep instead of raze
                    if (grid.GridSizeEnum == MyCubeSize.Small && 
                        block.FatBlock is IMyGasGenerator)
                    {
                        if (_random.NextDouble() < 0.5) // 50% chance
                        {
                            // Keep and damage this generator
                            float targetIntegrity = 0.2f + ((float)_random.NextDouble() * 0.5f);
                            float currentIntegrity = block.BuildLevelRatio;
                            float damageNeeded = (currentIntegrity - targetIntegrity) * block.MaxIntegrity;
                            
                            if (damageNeeded > 0)
                            {
                                var hitInfo = new MyHitInfo();
                                hitInfo.Position = block.Position;
                                hitInfo.Normal = Vector3.Up;
                                hitInfo.Velocity = Vector3.Zero;
                                
                                block.DoDamage(
                                    damageNeeded, 
                                    MyStringHash.GetOrCompute("Decay"), 
                                    sync: true, 
                                    hitInfo: hitInfo,
                                    attackerId: 0L,
                                    shouldDetonateAmmo: false
                                );
                            }
                            
                            // Mark as processed
                            if (terminalBlock != null)
                            {
                                terminalBlock.CustomData = "\u200B";
                            }
                            
                            if (DEBUG_MODE)
                                Log($"[DEBUG] Keeping O2/H2 Generator (50% chance) at {targetIntegrity*100:F0}%");
                            
                            continue; // Don't add to raze list
                        }
                    }
                    
                    blocksToRaze.Add(block);
                }
            }

            if (blocksToRaze.Count > 0)
            {
                _processingQueue[grid.EntityId] = new GridProcessingState
                {
                    Grid = grid,
                    BlocksToKeep = keptBlocks,
                    BlocksToRaze = blocksToRaze,
                    CurrentRazeIndex = 0
                };
                
                if (DEBUG_MODE)
                    Log($"[DEBUG] Queued {grid.DisplayName}: {blocksToRaze.Count} blocks to raze, {keptBlocks.Count} to keep");
            }
            
            // No need to unsubscribe - we never subscribe to OnGridSplit anymore
        }

        private void ProcessQueue()
        {
            try
            {
                if (_processingQueue == null || _processingQueue.Count == 0)
                    return;

                var gridsToRemove = new List<long>();

            foreach (var kvp in _processingQueue)
            {
                var state = kvp.Value;
                
                if (state.Grid == null || state.Grid.Closed || state.Grid.MarkedForClose)
                {
                    // Grid was closed/deleted - clear our references and remove from queue
                    if (state.BlocksToRaze != null)
                        state.BlocksToRaze.Clear();
                    if (state.BlocksToKeep != null)
                        state.BlocksToKeep.Clear();
                    gridsToRemove.Add(kvp.Key);
                    continue;
                }

                // Raze a batch of blocks this frame
                int blocksRazedThisFrame = 0;
                while (state.CurrentRazeIndex < state.BlocksToRaze.Count && blocksRazedThisFrame < BLOCKS_TO_RAZE_PER_FRAME)
                {
                    try
                    {
                        var block = state.BlocksToRaze[state.CurrentRazeIndex];
                        
                        // Check if block still exists and belongs to a valid grid (any grid)
                        if (block != null && !block.IsDestroyed && 
                            block.CubeGrid != null && !block.CubeGrid.Closed && !block.CubeGrid.MarkedForClose)
                        {
                            // Use RazeBlock for proper physics cleanup (RemoveBlock leaves ghost collisions)
                            block.CubeGrid.RazeBlock(block.Position);
                            blocksRazedThisFrame++;
                        }
                        else if (DEBUG_MODE && block != null)
                        {
                            if (block.IsDestroyed)
                                Log($"[DEBUG] Block already destroyed (environment damage?), skipping");
                            else if (block.CubeGrid == null || block.CubeGrid.Closed)
                                Log($"[DEBUG] Block's grid was deleted (trash collector?), skipping");
                        }
                    }
                    catch (Exception ex)
                    {
                        if (DEBUG_MODE)
                            Log($"[DEBUG] Failed to remove block: {ex.Message}");
                    }
                    
                    state.CurrentRazeIndex++;
                }

                // Check if done
                if (state.CurrentRazeIndex >= state.BlocksToRaze.Count)
                {
                    if (DEBUG_MODE)
                        Log($"[DEBUG] Completed processing {state.Grid.DisplayName}: Razed {state.BlocksToRaze.Count} blocks, kept {state.BlocksToKeep.Count}");
                    
                    // Clear references to blocks
                    state.BlocksToRaze.Clear();
                    state.BlocksToKeep.Clear();
                    
                    gridsToRemove.Add(kvp.Key);
                }
                
                // Only process one grid per update to spread the load
                break;
            }

            // Remove completed grids from queue
            foreach (var gridId in gridsToRemove)
            {
                GridProcessingState state;
                if (_processingQueue.TryGetValue(gridId, out state))
                {
                    // No need to unsubscribe - already done in QueueGridForProcessing
                }
                _processingQueue.Remove(gridId);
            }
            }
            catch (Exception ex)
            {
                Log($"Error in ProcessQueue: {ex}");
            }
        }

        private void OnGridSplit(IMyCubeGrid originalGrid, IMyCubeGrid newGrid)
        {
            // DISABLED: We unsubscribe immediately after queuing, so this should never fire
            // Leaving stub in case we need to debug
            if (DEBUG_MODE)
                Log($"[DEBUG] WARNING: OnGridSplit fired but we should have unsubscribed! {newGrid.DisplayName}");
        }

        private void ApplyRustToGrid(IMyCubeGrid grid)
        {
            try
            {
                var gridInternal = grid as MyCubeGrid;
                if (gridInternal == null) return;

                var rustHash = MyStringHash.GetOrCompute("Rusty_Armor");
                var allBlocks = new List<IMySlimBlock>();
                grid.GetBlocks(allBlocks);

                foreach (var block in allBlocks)
                {
                    if (block?.FatBlock != null)
                    {
                        try
                        {
                            MyCube myCube;
                            if (gridInternal.TryGetCube(block.Position, out myCube) && myCube?.CubeBlock != null)
                            {
                                gridInternal.ChangeColorAndSkin(myCube.CubeBlock, skinSubtypeId: rustHash);
                            }
                        }
                        catch
                        {
                            // Silently ignore paint failures
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log($"Error applying rust to {grid.DisplayName}: {ex}");
            }
        }

        private void DisableAllPower(IMyCubeGrid grid)
        {
            var blocks = new List<IMySlimBlock>();
            grid.GetBlocks(blocks, b => b.FatBlock is IMyPowerProducer);
            
            foreach (var block in blocks)
            {
                var powerBlock = block.FatBlock as IMyFunctionalBlock;
                if (powerBlock != null)
                {
                    powerBlock.Enabled = false;
                }
            }
        }

        private bool IsValuableBlock(IMySlimBlock block)
        {
            if (block?.FatBlock == null || block.BlockDefinition == null) 
                return false;
            
            var cubeBlockDef = block.BlockDefinition as MyCubeBlockDefinition;
            if (cubeBlockDef?.Components == null)
                return false;
            
            // A block is "valuable" if it requires ScrapConstructionFrame to build
            // This means it's restricted by the scrapyard mod = not buildable = worth salvaging
            foreach (var component in cubeBlockDef.Components)
            {
                if (component.Definition?.Id.SubtypeName == "ScrapConstructionFrame")
                {
                    return true; // Restricted block = valuable
                }
            }
            
            return false; // Buildable block = not valuable
        }

        private bool IsExcludedBlock(IMySlimBlock block)
        {
            if (block == null || block.BlockDefinition == null)
                return true;

            // Check if it's a special content block (Unknown Signal grids)
            if (block.FatBlock != null)
            {
                var terminalBlock = block.FatBlock as IMyTerminalBlock;
                if (terminalBlock != null)
                {
                    string customName = terminalBlock.CustomName ?? "";
                    if (customName.Contains("Special Content") ||
                        customName.Contains("DeleteThisBlock"))
                        return true;
                }
            }

            string subtypeName = block.BlockDefinition.Id.SubtypeName;
            return subtypeName.Contains("Armor") || 
                   subtypeName.Contains("Wheel") || 
					subtypeName.Contains("Suspension") ||
                   subtypeName.Contains("Mag") || 
                   subtypeName.Contains("Landing") || 
                   subtypeName.Contains("Catwalk") || 
                   subtypeName.Contains("Attach") ||
                   subtypeName.Contains("Piston") ||
                   subtypeName.Contains("Rotor") ||
                   subtypeName.Contains("Hinge") ||
                   subtypeName.Contains("Conveyor") ||
                   subtypeName.Contains("StorageShelf") ||
                   subtypeName.Contains("Barrel") ||
                   subtypeName.Contains("Ladder") ||
                   subtypeName.Contains("Ramp") ||
                   subtypeName.Contains("Stair") ||
                   subtypeName.Contains("Window") ||
                   subtypeName.Contains("pipe") ||
                   subtypeName.Contains("Freight");
        }

        private bool IsExcludedSpecialGrid(IMyCubeGrid grid)
        {
            if (grid == null) return false;
            
            string name = grid.DisplayName?.ToLower() ?? "";
            
            // Exclude signal grids (by name)
            if (name.Contains("unknown signal") || 
                name.Contains("strong signal") ||
                name.Contains("unidentified signal"))
            {
                return true;
            }
            
            // Exclude spawn helper grids (by name)
            if (name.Contains("dummy grid") ||
                name.Contains("cluster core"))
            {
                return true;
            }
            
            // Exclude Factorum grids only
            foreach (var ownerId in grid.BigOwners)
            {
                var faction = MyAPIGateway.Session.Factions.TryGetPlayerFaction(ownerId);
                if (faction != null && faction.Tag == "FCTM")
                {
                    return true;
                }
            }
            
            return false;
        }

        private bool IsPlayerOwnedGrid(IMyCubeGrid grid)
        {
            if (grid == null || grid.BigOwners == null || grid.BigOwners.Count == 0)
                return false;

            foreach (var ownerId in grid.BigOwners)
            {
                if (IsPlayerIdentity(ownerId))
                    return true;
            }
            
            return false;
        }

        private bool IsPlayerIdentity(long identityId)
        {
            if (identityId == 0) return false;
            
            // Check all online players
            List<IMyPlayer> players = new List<IMyPlayer>();
            MyAPIGateway.Players.GetPlayers(players);
            
            foreach (var player in players)
            {
                if (player.IdentityId == identityId)
                    return true;
            }
            
            return false;
        }

        private void Log(string message)
        {
            MyLog.Default.WriteLine($"[Rustvirus] {message}");
        }
    }
}
