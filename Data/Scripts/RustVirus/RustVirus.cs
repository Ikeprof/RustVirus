using System;
using Sandbox.Game.EntityComponents;
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
    public class RustvirusV3 : MySessionComponentBase
    {
        private readonly Random _random = new Random();

        // ===== TUNABLES =====
        private const bool DEBUG_MODE = true;

        private const int UPDATE_RATE = 60;                 // process raze queue every 60 ticks (~1s)
        private const int BLOCKS_TO_RAZE_PER_FRAME = 20;   // batch raze to reduce physics spikes

        private const int SPAWN_DELAY_SECONDS = 10;         // per-grid delay to let spawners finish setup
        private const int STARTUP_GRACE_SECONDS = 60;       // global delay after world start (restart safety)
        private const int NOT_READY_RETRY_SECONDS = 5;      // if grid not ready (physics/group), re-check later

        // Pure armor debris chunks at or below this block count get deleted immediately on split.
        // Anything larger is kept for immersion — it's still recognizable as part of the ship.
        private const int MAX_DEBRIS_BLOCKS_TO_DELETE = 60;

        // How long we monitor surviving split halves for further cascading splits.
        // After this window, physics has done its worst and we stop watching.
        private const int SPLIT_TRACKING_EXPIRY_SECONDS = 60;

        // ===== PERSISTENT MARKER (ModStorage) =====
        // One static GUID for the mod. Do NOT change once deployed, unless you intentionally want to forget old marks.
        private static readonly Guid RV_MARK_GUID = new Guid("2d4c3f0e-7f2a-4f4a-9dfb-6b9c6a5b9f01");
        private const string RV_MARK_VALUE = "1";

        // ===== STATE =====
        private bool _simStarted = false;
        private DateTime _worldStartUtc;

        // ===== SPLIT TRACKING =====
        private readonly HashSet<long> _splitTrackedGridIds = new HashSet<long>();
        private readonly Dictionary<long, DateTime> _splitTrackingExpiry = new Dictionary<long, DateTime>();
        private readonly Dictionary<long, DateTime> _processAfterUtcByGridId = new Dictionary<long, DateTime>();
        private readonly Dictionary<long, GridProcessingState> _processingQueue = new Dictionary<long, GridProcessingState>();

        private int _updateCounter = 0;

        private class GridProcessingState
        {
            public IMyCubeGrid Grid;
            public HashSet<IMySlimBlock> BlocksToKeep;
            public List<IMySlimBlock> BlocksToRaze;
            public int CurrentRazeIndex;
        }

        private enum QueueResult
        {
            Queued,
            IgnoreForever,
            RetryLater
        }

        // ===== LIFECYCLE =====

        public override void LoadData()
        {
            if (!MyAPIGateway.Multiplayer.IsServer)
                return;

            MyAPIGateway.Entities.OnEntityAdd += OnEntityAdded;
            MyAPIGateway.Entities.OnEntityRemove += OnEntityRemoved;
        }

        protected override void UnloadData()
        {
            if (!MyAPIGateway.Multiplayer.IsServer)
                return;

            MyAPIGateway.Entities.OnEntityAdd -= OnEntityAdded;
            MyAPIGateway.Entities.OnEntityRemove -= OnEntityRemoved;
        }

        public override void UpdateBeforeSimulation()
        {
            if (!MyAPIGateway.Multiplayer.IsServer)
                return;

            if (!_simStarted)
            {
                _simStarted = true;
                _worldStartUtc = DateTime.UtcNow;
                Log($"Simulation started. Startup grace={STARTUP_GRACE_SECONDS}s");
                return;
            }

            if (!InStartupGrace())
            {
                EvaluatePendingGrids();
            }

            if (++_updateCounter % UPDATE_RATE == 0)
            {
                ProcessQueue();
                ExpireSplitTracking();
            }
        }

        private bool InStartupGrace()
        {
            return (DateTime.UtcNow - _worldStartUtc).TotalSeconds < STARTUP_GRACE_SECONDS;
        }

        // ===== ENTITY EVENTS =====

        private void OnEntityAdded(IMyEntity entity)
        {
            try
            {
                var grid = entity as IMyCubeGrid;
                if (grid == null) return;

                var now = DateTime.UtcNow;
                var perGridReady = now.AddSeconds(SPAWN_DELAY_SECONDS);
                var globalReady = _worldStartUtc.AddSeconds(STARTUP_GRACE_SECONDS);
                var processAfter = perGridReady > globalReady ? perGridReady : globalReady;

                DateTime existing;
                if (_processAfterUtcByGridId.TryGetValue(grid.EntityId, out existing))
                {
                    if (processAfter > existing)
                        _processAfterUtcByGridId[grid.EntityId] = processAfter;
                }
                else
                {
                    _processAfterUtcByGridId[grid.EntityId] = processAfter;
                }

                if (DEBUG_MODE)
                    Log($"[ADD] id={grid.EntityId} name=\"{grid.DisplayName}\" size={grid.GridSizeEnum} owners={grid.BigOwners?.Count ?? 0} processAfter={processAfter:O}");
            }
            catch (Exception ex)
            {
                Log($"Error in OnEntityAdded: {ex}");
            }
        }

        private void OnEntityRemoved(IMyEntity entity)
        {
            try
            {
                var grid = entity as IMyCubeGrid;
                if (grid == null) return;

                if (_splitTrackedGridIds.Contains(grid.EntityId))
                {
                    grid.OnGridSplit -= OnGridSplit;
                    _splitTrackedGridIds.Remove(grid.EntityId);
                    _splitTrackingExpiry.Remove(grid.EntityId);

                    if (DEBUG_MODE)
                        Log($"[SPLIT] Unregistered split handler (entity removed): id={grid.EntityId} name=\"{grid.DisplayName}\"");
                }
            }
            catch (Exception ex)
            {
                Log($"Error in OnEntityRemoved: {ex}");
            }
        }

        // ===== SPLIT TRACKING =====

        private void RegisterSplitTrackingForRaze(IMyCubeGrid grid)
        {
            if (grid == null) return;
            if (_splitTrackedGridIds.Contains(grid.EntityId)) return;

            grid.OnGridSplit += OnGridSplit;
            _splitTrackedGridIds.Add(grid.EntityId);

            if (DEBUG_MODE)
                Log($"[SPLIT] Registered split handler (raze): id={grid.EntityId} name=\"{grid.DisplayName}\"");
        }

        private void RegisterSplitTrackingForSurvivingHalf(IMyCubeGrid grid)
        {
            if (grid == null) return;

            if (!_splitTrackedGridIds.Contains(grid.EntityId))
            {
                grid.OnGridSplit += OnGridSplit;
                _splitTrackedGridIds.Add(grid.EntityId);
            }

            _splitTrackingExpiry[grid.EntityId] = DateTime.UtcNow.AddSeconds(SPLIT_TRACKING_EXPIRY_SECONDS);

            if (DEBUG_MODE)
                Log($"[SPLIT] Registered split handler (surviving half, expires {SPLIT_TRACKING_EXPIRY_SECONDS}s): id={grid.EntityId} name=\"{grid.DisplayName}\"");
        }

        private void UnregisterSplitTracking(IMyCubeGrid grid)
        {
            if (grid == null) return;
            if (!_splitTrackedGridIds.Contains(grid.EntityId)) return;

            grid.OnGridSplit -= OnGridSplit;
            _splitTrackedGridIds.Remove(grid.EntityId);
            _splitTrackingExpiry.Remove(grid.EntityId);

            if (DEBUG_MODE)
                Log($"[SPLIT] Unregistered split handler: id={grid.EntityId} name=\"{grid.DisplayName}\"");
        }

        private void ExpireSplitTracking()
        {
            if (_splitTrackingExpiry.Count == 0)
                return;

            var now = DateTime.UtcNow;
            var expired = _splitTrackingExpiry.Where(kvp => now >= kvp.Value).Select(kvp => kvp.Key).ToList();

            foreach (var gridId in expired)
            {
                _splitTrackingExpiry.Remove(gridId);
                _splitTrackedGridIds.Remove(gridId);

                IMyEntity entity;
                if (MyAPIGateway.Entities.TryGetEntityById(gridId, out entity))
                {
                    var grid = entity as IMyCubeGrid;
                    if (grid != null)
                    {
                        grid.OnGridSplit -= OnGridSplit;
                        if (DEBUG_MODE)
                            Log($"[SPLIT] Expired split tracking: id={gridId} name=\"{grid.DisplayName}\"");
                    }
                }
                else
                {
                    if (DEBUG_MODE)
                        Log($"[SPLIT] Expired split tracking (grid already gone): id={gridId}");
                }
            }
        }

        private void OnGridSplit(IMyCubeGrid originalGrid, IMyCubeGrid newGrid)
        {
            try
            {
                if (!MyAPIGateway.Multiplayer.IsServer) return;

                if (DEBUG_MODE)
                    Log($"[SPLIT] Detected: original=\"{originalGrid?.DisplayName}\" new=\"{newGrid?.DisplayName}\"");

                TryDeleteDebrisGrid(originalGrid, "original");
                TryDeleteDebrisGrid(newGrid, "new");
            }
            catch (Exception ex)
            {
                Log($"Error in OnGridSplit: {ex}");
            }
        }

        private void TryDeleteDebrisGrid(IMyCubeGrid grid, string label)
        {
            if (grid == null || grid.Closed || grid.MarkedForClose)
                return;

            if (IsPlayerOwnedMechanicalGroup(grid))
            {
                if (DEBUG_MODE)
                    Log($"[SPLIT] {label} half is player-owned, skipping: \"{grid.DisplayName}\"");
                return;
            }

            var functionalBlocks = new List<IMySlimBlock>();
            grid.GetBlocks(functionalBlocks, b => b != null && b.FatBlock != null && !IsExcludedBlock(b));

            if (functionalBlocks.Count > 0)
            {
                RegisterSplitTrackingForSurvivingHalf(grid);
                if (DEBUG_MODE)
                    Log($"[SPLIT] {label} half has {functionalBlocks.Count} functional blocks, registering for further splits: \"{grid.DisplayName}\"");
                return;
            }

            var allBlocks = new List<IMySlimBlock>();
            grid.GetBlocks(allBlocks);

            if (allBlocks.Count > MAX_DEBRIS_BLOCKS_TO_DELETE)
            {
                RegisterSplitTrackingForSurvivingHalf(grid);
                if (DEBUG_MODE)
                    Log($"[SPLIT] {label} half is pure armor but {allBlocks.Count} blocks > {MAX_DEBRIS_BLOCKS_TO_DELETE}, keeping for immersion: \"{grid.DisplayName}\"");
                return;
            }

            if (DEBUG_MODE)
                Log($"[SPLIT] {label} half is pure armor debris ({allBlocks.Count} blocks), deleting: \"{grid.DisplayName}\"");

            try
            {
                grid.Close();
            }
            catch (Exception ex)
            {
                Log($"[SPLIT] Failed to close debris grid \"{grid.DisplayName}\": {ex.Message}");
            }
        }

        // ===== EVALUATE / QUEUE =====

        private void EvaluatePendingGrids()
        {
            if (_processAfterUtcByGridId.Count == 0)
                return;

            var now = DateTime.UtcNow;
            var gridIds = _processAfterUtcByGridId.Keys.ToList();

            foreach (var gridId in gridIds)
            {
                DateTime processAfter;
                if (!_processAfterUtcByGridId.TryGetValue(gridId, out processAfter))
                    continue;

                if (now < processAfter)
                    continue;

                IMyEntity entity;
                if (!MyAPIGateway.Entities.TryGetEntityById(gridId, out entity) || entity == null)
                {
                    _processAfterUtcByGridId.Remove(gridId);
                    continue;
                }

                var grid = entity as IMyCubeGrid;
                if (grid == null || grid.Closed || grid.MarkedForClose)
                {
                    _processAfterUtcByGridId.Remove(gridId);
                    continue;
                }

                if (grid.Physics == null)
                {
                    _processAfterUtcByGridId[gridId] = now.AddSeconds(NOT_READY_RETRY_SECONDS);
                    if (DEBUG_MODE) Log($"[DEFER] id={gridId} name=\"{grid.DisplayName}\" reason=PhysicsNull retry={NOT_READY_RETRY_SECONDS}s");
                    continue;
                }

                if (IsExcludedSpecialGrid(grid))
                {
                    if (DEBUG_MODE) Log($"[SKIP] id={gridId} name=\"{grid.DisplayName}\" reason=SpecialGrid");
                    _processAfterUtcByGridId.Remove(gridId);
                    continue;
                }

                if (IsPlayerOwnedMechanicalGroup(grid))
                {
                    if (DEBUG_MODE) Log($"[SKIP] id={gridId} name=\"{grid.DisplayName}\" reason=PlayerMechanicalGroup");
                    _processAfterUtcByGridId.Remove(gridId);
                    continue;
                }

                if (IsFactorumMechanicalGroup(grid))
                {
                    if (DEBUG_MODE) Log($"[SKIP] id={gridId} name=\"{grid.DisplayName}\" reason=FactorumMechanicalGroup");
                    _processAfterUtcByGridId.Remove(gridId);
                    continue;
                }

                if (IsMarkedEntity(entity))
                {
                    if (DEBUG_MODE) Log($"[SKIP] id={gridId} name=\"{grid.DisplayName}\" reason=GridAlreadyMarked");
                    _processAfterUtcByGridId.Remove(gridId);
                    continue;
                }

                Log($"[QUEUE] NPC/Nobody grid for salvage: id={gridId} name=\"{grid.DisplayName}\"");
                var result = QueueGridForProcessing(grid);

                if (result == QueueResult.Queued || result == QueueResult.IgnoreForever)
                {
                    MarkEntity(entity);
                    _processAfterUtcByGridId.Remove(gridId);
                }
                else
                {
                    _processAfterUtcByGridId[gridId] = now.AddSeconds(NOT_READY_RETRY_SECONDS);
                    if (DEBUG_MODE) Log($"[DEFER] id={gridId} name=\"{grid.DisplayName}\" reason=RetryLater retry={NOT_READY_RETRY_SECONDS}s");
                }
            }
        }

        private QueueResult QueueGridForProcessing(IMyCubeGrid grid)
        {
            if (grid == null)
                return QueueResult.RetryLater;

            if (_processingQueue.ContainsKey(grid.EntityId))
                return QueueResult.Queued;

            RegisterSplitTrackingForRaze(grid);
            DisableAllPower(grid);

            var blocks = new List<IMySlimBlock>();
            grid.GetBlocks(blocks, block => block != null && block.FatBlock != null && !IsExcludedBlock(block));

            if (blocks.Count == 0)
            {
                if (DEBUG_MODE) Log($"[INFO] Armor-only grid, ignore forever: {grid.DisplayName}");
                UnregisterSplitTracking(grid);
                return QueueResult.IgnoreForever;
            }

            var valuableBlocks = blocks.Where(b =>
            {
                if (!IsValuableBlock(b))
                    return false;

                var tb = b.FatBlock as IMyTerminalBlock;
                if (tb != null && IsMarkedTerminal(tb))
                {
                    if (DEBUG_MODE) Log($"[DEBUG] Valuable block already marked, skipping: {Subtype(b)}");
                    return false;
                }

                return true;
            }).ToList();

            if (DEBUG_MODE)
                Log($"[DEBUG] {grid.DisplayName}: functional={blocks.Count} valuableUnmarked={valuableBlocks.Count}");

            int keepCount = grid.GridSizeEnum == MyCubeSize.Small
                ? Math.Min(10, valuableBlocks.Count)
                : Math.Min(5, valuableBlocks.Count);

            var kept = new HashSet<IMySlimBlock>();

            if (valuableBlocks.Count > 0)
            {
                var powerBlocks = valuableBlocks.Where(b => b.FatBlock is IMyPowerProducer).ToList();
                if (powerBlocks.Count > 0)
                {
                    var powerBlock = powerBlocks[_random.Next(powerBlocks.Count)];
                    kept.Add(powerBlock);
                    if (DEBUG_MODE) Log($"[DEBUG] Priority power keep: {Subtype(powerBlock)}");
                }

                int remaining = keepCount - kept.Count;
                if (remaining > 0)
                {
                    var pool = valuableBlocks.Where(b => !kept.Contains(b)).OrderBy(_ => _random.Next()).Take(remaining);
                    foreach (var b in pool)
                        kept.Add(b);
                }

                foreach (var b in kept)
                {
                    DamageBlockToTargetIntegrity(b, 0.2f, 0.7f);

                    var tb = b.FatBlock as IMyTerminalBlock;
                    if (tb != null)
                        MarkTerminal(tb);
                }
            }

            var raze = new List<IMySlimBlock>();

            foreach (var b in blocks)
            {
                if (kept.Contains(b))
                    continue;

                var tb = b.FatBlock as IMyTerminalBlock;
                if (tb != null && IsMarkedTerminal(tb))
                {
                    if (DEBUG_MODE) Log($"[DEBUG] Not razing (marked): {Subtype(b)}");
                    continue;
                }

                if (grid.GridSizeEnum == MyCubeSize.Small && b.FatBlock is IMyGasGenerator)
                {
                    if (_random.NextDouble() < 0.5)
                    {
                        DamageBlockToTargetIntegrity(b, 0.2f, 0.7f);
                        if (tb != null) MarkTerminal(tb);
                        if (DEBUG_MODE) Log($"[DEBUG] Kept O2/H2 Gen (50%): {Subtype(b)}");
                        continue;
                    }
                }

                if (b.FatBlock is IMyProgrammableBlock)
                {
                    if (_random.NextDouble() < 0.5)
                    {
                        DamageBlockToTargetIntegrity(b, 0.2f, 0.7f);
                        if (tb != null) MarkTerminal(tb);
                        if (DEBUG_MODE) Log($"[DEBUG] Kept Programmable Block (50%): {Subtype(b)}");
                        continue;
                    }
                }

                raze.Add(b);
            }

            if (raze.Count == 0)
            {
                if (DEBUG_MODE) Log($"[INFO] Nothing to raze on {grid.DisplayName}");
                UnregisterSplitTracking(grid);
                return QueueResult.IgnoreForever;
            }

            _processingQueue[grid.EntityId] = new GridProcessingState
            {
                Grid = grid,
                BlocksToKeep = kept,
                BlocksToRaze = raze,
                CurrentRazeIndex = 0
            };

            if (DEBUG_MODE)
                Log($"[DEBUG] Queued {grid.DisplayName}: raze={raze.Count} keep={kept.Count}");

            return QueueResult.Queued;
        }

        // ===== PROCESS QUEUE =====

        private void ProcessQueue()
        {
            try
            {
                if (_processingQueue.Count == 0)
                    return;

                var gridsToRemove = new List<long>();

                foreach (var kvp in _processingQueue)
                {
                    var gridId = kvp.Key;
                    var state = kvp.Value;

                    if (state.Grid == null || state.Grid.Closed || state.Grid.MarkedForClose)
                    {
                        UnregisterSplitTracking(state.Grid);
                        SafeClearState(state);
                        gridsToRemove.Add(gridId);
                        continue;
                    }

                    int razed = 0;

                    while (state.CurrentRazeIndex < state.BlocksToRaze.Count && razed < BLOCKS_TO_RAZE_PER_FRAME)
                    {
                        var b = state.BlocksToRaze[state.CurrentRazeIndex];
                        state.CurrentRazeIndex++;

                        if (b == null || b.IsDestroyed)
                            continue;

                        var cg = b.CubeGrid;
                        if (cg == null || cg.Closed || cg.MarkedForClose)
                            continue;

                        if (IsPlayerOwnedMechanicalGroup(cg))
                        {
                            if (DEBUG_MODE)
                                Log($"[SAFETY] Skip raze (player mech group): grid=\"{cg.DisplayName}\" block={Subtype(b)}");
                            continue;
                        }

                        try
                        {
							if (DEBUG_MODE)
							Log($"[DEBUG] Razing: {Subtype(b)}");
                            cg.RazeBlock(b.Position);
                            razed++;
                        }
                        catch (Exception ex)
                        {
                            if (DEBUG_MODE)
                                Log($"[DEBUG] Raze failed: {ex.Message}");
                        }
                    }

                    if (state.CurrentRazeIndex >= state.BlocksToRaze.Count)
                    {
                        if (DEBUG_MODE)
                            Log($"[DONE] {state.Grid.DisplayName}: razed={state.BlocksToRaze.Count} kept={state.BlocksToKeep.Count}");

                        UnregisterSplitTracking(state.Grid);
                        SafeClearState(state);
                        gridsToRemove.Add(gridId);
                    }

                    break;
                }

                foreach (var gid in gridsToRemove)
                {
                    _processingQueue.Remove(gid);
                }
            }
            catch (Exception ex)
            {
                Log($"Error in ProcessQueue: {ex}");
            }
        }

        private void SafeClearState(GridProcessingState state)
        {
            try
            {
                state?.BlocksToRaze?.Clear();
                state?.BlocksToKeep?.Clear();
            }
            catch { }
        }

        // ===== DAMAGE =====

        private void DamageBlockToTargetIntegrity(IMySlimBlock block, float minTargetRatio, float maxTargetRatio)
        {
            if (block == null)
                return;

            float targetRatio = minTargetRatio + (float)_random.NextDouble() * (maxTargetRatio - minTargetRatio);

            float max = block.MaxIntegrity;
            float cur = block.Integrity;
            if (max <= 0f) return;

            float curRatio = cur / max;
            if (curRatio <= targetRatio)
                return;

            float damageNeeded = (curRatio - targetRatio) * max;
            if (damageNeeded <= 0f)
                return;

            var hitInfo = new MyHitInfo
            {
                Position = block.CubeGrid.GridIntegerToWorld(block.Position),
                Normal = Vector3D.Up,
                Velocity = Vector3D.Zero
            };

            block.DoDamage(
                damageNeeded,
                MyStringHash.GetOrCompute("Decay"),
                sync: true,
                hitInfo: hitInfo,
                attackerId: 0L,
                shouldDetonateAmmo: false
            );

            if (DEBUG_MODE)
                Log($"[DEBUG] Damaged keep: {Subtype(block)} target={targetRatio * 100f:F0}% cur={curRatio * 100f:F0}%");
        }

        // ===== POWER =====

        private void DisableAllPower(IMyCubeGrid grid)
        {
            var blocks = new List<IMySlimBlock>();
            grid.GetBlocks(blocks, b => b?.FatBlock is IMyPowerProducer);

            foreach (var b in blocks)
            {
                var fb = b.FatBlock as IMyFunctionalBlock;
                if (fb != null)
                    fb.Enabled = false;
            }
        }

        // ===== OWNERSHIP / GROUPING =====

        private bool IsPlayerOwnedMechanicalGroup(IMyCubeGrid grid)
        {
            try
            {
                if (grid == null) return false;

                var group = new List<IMyCubeGrid>();
                MyAPIGateway.GridGroups.GetGroup(grid, GridLinkTypeEnum.Mechanical, group);

                if (group.Count == 0)
                    group.Add(grid);

                foreach (var g in group)
                {
                    if (IsPlayerOwnedGridByIdentity(g))
                        return true;
                }

                return false;
            }
            catch
            {
                return IsPlayerOwnedGridByIdentity(grid);
            }
        }

        private bool IsFactorumMechanicalGroup(IMyCubeGrid grid)
        {
            try
            {
                if (grid == null) return false;

                var group = new List<IMyCubeGrid>();
                MyAPIGateway.GridGroups.GetGroup(grid, GridLinkTypeEnum.Mechanical, group);
                if (group.Count == 0) group.Add(grid);

                foreach (var g in group)
                {
                    if (IsFactorumGrid(g))
                        return true;
                }
            }
            catch { }

            return false;
        }

        private bool IsFactorumGrid(IMyCubeGrid grid)
        {
            if (grid == null) return false;
            if (grid.BigOwners == null) return false;

            foreach (var ownerId in grid.BigOwners)
            {
                var faction = MyAPIGateway.Session?.Factions?.TryGetPlayerFaction(ownerId);
                if (faction != null && faction.Tag == "FCTM")
                    return true;
            }
            return false;
        }

        private bool IsPlayerOwnedGridByIdentity(IMyCubeGrid grid)
        {
            if (grid == null || grid.BigOwners == null || grid.BigOwners.Count == 0)
                return false;

            foreach (var ownerId in grid.BigOwners)
            {
                if (IsRealPlayerIdentity(ownerId))
                    return true;
            }

            return false;
        }

        private bool IsRealPlayerIdentity(long identityId)
        {
            if (identityId == 0) return false;

            try
            {
                ulong steamId = MyAPIGateway.Players.TryGetSteamId(identityId);
                return steamId != 0;
            }
            catch
            {
                return false;
            }
        }

        // ===== VALUE / EXCLUSIONS =====

        private bool IsValuableBlock(IMySlimBlock block)
        {
            if (block?.FatBlock == null || block.BlockDefinition == null)
                return false;

            var def = block.BlockDefinition as MyCubeBlockDefinition;
            if (def?.Components == null)
                return false;

            foreach (var c in def.Components)
            {
                if (c.Definition?.Id.SubtypeName == "ScrapConstructionFrame")
                    return true;
            }

            return false;
        }

        private bool IsExcludedBlock(IMySlimBlock block)
        {
            if (block == null || block.BlockDefinition == null)
                return true;

            if (block.FatBlock != null)
            {
                var tb = block.FatBlock as IMyTerminalBlock;
                if (tb != null)
                {
                    string n = tb.CustomName ?? "";
                    if (n.Contains("Special Content") || n.Contains("DeleteThisBlock"))
                        return true;
                }
            }

            string subtype = block.BlockDefinition.Id.SubtypeName ?? "";

            return subtype.Contains("Armor") ||
                   subtype.Contains("Wheel") ||
                   subtype.Contains("Suspension") ||
                   subtype.Contains("Mag") ||
                   subtype.Contains("Landing") ||
                   subtype.Contains("Catwalk") ||
                   subtype.Contains("Attach") ||
                   subtype.Contains("Piston") ||
                   subtype.Contains("Rotor") ||
                   subtype.Contains("Hinge") ||
                   subtype.Contains("Conveyor") ||
                   subtype.Contains("StorageShelf") ||
                   subtype.Contains("Barrel") ||
                   subtype.Contains("Ladder") ||
                   subtype.Contains("Ramp") ||
                   subtype.Contains("Stair") ||
                   subtype.Contains("Window") ||
                   subtype.Contains("pipe") ||
                   subtype.Contains("Freight");
        }

        private bool IsExcludedSpecialGrid(IMyCubeGrid grid)
        {
            if (grid == null) return false;

            string name = (grid.DisplayName ?? "").ToLower();

            if (name.Contains("unknown signal") ||
                name.Contains("strong signal") ||
                name.Contains("large loot") ||
                name.Contains("small loot") ||
                name.Contains("[lb ") ||
                name.Contains("unidentified signal"))
                return true;

            if (name.Contains("dummy grid") ||
                name.Contains("cluster core"))
                return true;

            return false;
        }

        // ===== MODSTORAGE MARKERS =====

        private bool IsMarkedTerminal(IMyTerminalBlock tb)
        {
            try
            {
                if (tb == null) return false;
                if (tb.Storage == null) return false;

                string v;
                return tb.Storage.TryGetValue(RV_MARK_GUID, out v) && v == RV_MARK_VALUE;
            }
            catch { return false; }
        }

        private void MarkTerminal(IMyTerminalBlock tb)
        {
            try
            {
                if (tb == null) return;
                if (tb.Storage == null) tb.Storage = new MyModStorageComponent();
                tb.Storage[RV_MARK_GUID] = RV_MARK_VALUE;
            }
            catch { }
        }

        private bool IsMarkedEntity(IMyEntity entity)
        {
            try
            {
                if (entity == null) return false;
                if (entity.Storage == null) return false;

                string v;
                return entity.Storage.TryGetValue(RV_MARK_GUID, out v) && v == RV_MARK_VALUE;
            }
            catch { return false; }
        }

        private void MarkEntity(IMyEntity entity)
        {
            try
            {
                if (entity == null) return;
                if (entity.Storage == null) entity.Storage = new MyModStorageComponent();
                entity.Storage[RV_MARK_GUID] = RV_MARK_VALUE;
            }
            catch { }
        }

        // ===== UTIL =====

        private string Subtype(IMySlimBlock b)
        {
            try
            {
                return b?.BlockDefinition?.Id.SubtypeName ?? "Unknown";
            }
            catch { return "Unknown"; }
        }

        private void Log(string message)
        {
            MyLog.Default.WriteLine($"[Rustvirus] {message}");
        }
    }
}
