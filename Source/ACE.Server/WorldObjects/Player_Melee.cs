using System;
using System.Collections.Generic;

using ACE.DatLoader.Entity.AnimationHooks;
using ACE.Entity.Enum;
using ACE.Entity.Enum.Properties;
using ACE.Server.Entity;
using ACE.Server.Entity.Actions;
using ACE.Server.Factories;
using ACE.Server.Managers;
using ACE.Server.Network.GameEvent.Events;
using ACE.Server.Network.GameMessages.Messages;
using ACE.Server.Physics;
using ACE.Server.Physics.Animation;
using ACE.Server.Physics.Common;
using ACE.Server.Physics.Extensions;
using ACE.Entity;

using Position = ACE.Entity.Position;

namespace ACE.Server.WorldObjects
{
    /// <summary>
    /// Player melee attack
    /// </summary>
    partial class Player
    {

        public int? LumAugmentMeleeRange
        {
            get => GetProperty(PropertyInt.LumAugmentMeleeRange);
            set { if (!value.HasValue) RemoveProperty(PropertyInt.LumAugmentMeleeRange); else SetProperty(PropertyInt.LumAugmentMeleeRange, value.Value); }
        }

        public int? LumAugmentPowerBarSpeed
        {
            get => GetProperty(PropertyInt.LumAugmentPowerBarSpeed) ?? 0;
            set { if (!value.HasValue) RemoveProperty(PropertyInt.LumAugmentPowerBarSpeed); else SetProperty(PropertyInt.LumAugmentPowerBarSpeed, value.Value); }
        }

        /// <summary>
        /// The target this player is currently performing a melee attack on
        /// </summary>
        public Creature MeleeTarget;

        private float _powerLevel;

        /// <summary>
        /// The power bar level, a value between 0-1
        /// </summary>
        public float PowerLevel
        {
            get => IsExhausted ? 0.0f : _powerLevel;
            set => _powerLevel = value;
        }

        public override PowerAccuracy GetPowerRange()
        {
            if (PowerLevel < 0.33f)
                return PowerAccuracy.Low;
            else if (PowerLevel < 0.66f)
                return PowerAccuracy.Medium;
            else
                return PowerAccuracy.High;
        }

        public AttackQueue AttackQueue;

        /// <summary>
        /// Called when a player first initiates a melee attack
        /// </summary>
        public void HandleActionTargetedMeleeAttack(uint targetGuid, uint attackHeight, float powerLevel)
        {
            //log.Info($"-");

            if (CombatMode != CombatMode.Melee)
            {
                //log.Error($"{Name}.HandleActionTargetedMeleeAttack({targetGuid:X8}, {attackHeight}, {powerLevel}) - CombatMode mismatch {CombatMode}, LastCombatMode {LastCombatMode}");

                if (LastCombatMode == CombatMode.Melee)
                    CombatMode = CombatMode.Melee;
                else
                {
                    OnAttackDone();
                    return;
                }
            }

            if (IsBusy || Teleporting || suicideInProgress)
            {
                SendWeenieError(WeenieError.YoureTooBusy);
                OnAttackDone();
                return;
            }

            if (IsJumping)
            {
                SendWeenieError(WeenieError.YouCantDoThatWhileInTheAir);
                OnAttackDone();
                return;
            }

            if (PKLogout)
            {
                SendWeenieError(WeenieError.YouHaveBeenInPKBattleTooRecently);
                OnAttackDone();
                return;
            }

            // verify input
            powerLevel = Math.Clamp(powerLevel, 0.0f, 1.0f);

            AttackHeight = (AttackHeight)attackHeight;
            AttackQueue.Add(powerLevel);

            if (MeleeTarget == null)
                PowerLevel = AttackQueue.Fetch();

            // already in melee loop?
            if (Attacking || MeleeTarget != null && MeleeTarget.IsAlive)
                return;

            // get world object for target creature
            var target = CurrentLandblock?.GetObject(targetGuid);

            if (target == null)
            {
                //log.Debug($"{Name}.HandleActionTargetedMeleeAttack({targetGuid:X8}, {AttackHeight}, {powerLevel}) - couldn't find target guid");
                OnAttackDone();
                return;
            }

            var creatureTarget = target as Creature;
            if (creatureTarget == null)
            {
                log.Warn($"{Name}.HandleActionTargetedMeleeAttack({targetGuid:X8}, {AttackHeight}, {powerLevel}) - target guid not creature");
                OnAttackDone();
                return;
            }

            if (!CanDamage(creatureTarget))
            {
                SendTransientError($"You cannot attack {creatureTarget.Name}");
                OnAttackDone();
                return;
            }

            if (!creatureTarget.IsAlive)
            {
                OnAttackDone();
                return;
            }

            //log.Info($"{Name}.HandleActionTargetedMeleeAttack({targetGuid:X8}, {attackHeight}, {powerLevel})");

            // Check for Flicker Strike
            var weapon = GetEquippedMeleeWeapon();
            if (weapon != null && (weapon.GetProperty(PropertyBool.FlickerStrike) ?? false))
            {
                // Stamina Cost: 10%
                var staminaCost = (int)(Stamina.MaxValue * 0.10f);
                staminaCost = 10;

                if (Stamina.Current > staminaCost)
                {
                    UpdateVitalDelta(Stamina, -staminaCost);
                    LaunchFlickerProjectile(creatureTarget);
                    // Skip standard attack
                    // OnAttackDone();
                    return;
                }
                else
                {
                     SendTransientError("Not enough stamina for Flicker Strike!");
                }
            }

            MeleeTarget = creatureTarget;
            AttackTarget = MeleeTarget;

            // reset PrevMotionCommand / DualWieldAlternate each time button is clicked
            PrevMotionCommand = MotionCommand.Invalid;
            DualWieldAlternate = false;

            var attackSequence = ++AttackSequence;

            if (NextRefillTime > DateTime.UtcNow)
            {
                var delayTime = (float)(NextRefillTime - DateTime.UtcNow).TotalSeconds;

                var actionChain = new ActionChain();
                actionChain.AddDelaySeconds(delayTime);
                actionChain.AddAction(this, ActionType.PlayerMelee_HandleTargetedAttack, () =>
                {
                    if (!creatureTarget.IsAlive)
                    {
                        OnAttackDone();
                        return;
                    }

                    HandleActionTargetedMeleeAttack_Inner(target, attackSequence);
                });
                actionChain.EnqueueChain();
            }
            else
                HandleActionTargetedMeleeAttack_Inner(target, attackSequence);
        }

        public static readonly float MeleeDistance  = 0.6f;
        public static readonly float StickyDistance = 4.0f;
        public static readonly float RepeatDistance = 16.0f;

        public void HandleActionTargetedMeleeAttack_Inner(WorldObject target, int attackSequence)
        {
            var dist = GetCylinderDistance(target);
            //Log info for debugging Flicker Strike loop
            //if (GetProperty(PropertyBool.FlickerStrike) ?? false)
            //    Console.WriteLine($"[Melee] Dist: {dist}, MeleeRange: {MeleeDistance + (LumAugmentMeleeRange ?? 0) * 0.1f}");

            if (dist <= (MeleeDistance + (LumAugmentMeleeRange ?? 0) * 0.1f) || dist <= StickyDistance && IsMeleeVisible(target))
            {
                // sticky melee
                var angle = GetAngle(target);
                if (angle > ServerConfig.melee_max_angle.Value)
                {
                    var rotateTime = Rotate(target);

                    var actionChain = new ActionChain();
                    actionChain.AddDelaySeconds(rotateTime);
                    actionChain.AddAction(this, ActionType.PlayerMelee_Attack, () => Attack(target, attackSequence));
                    actionChain.EnqueueChain();
                }
                else
                    Attack(target, attackSequence);
            }
            else
            {
                // turn / move to required
                if (GetCharacterOption(CharacterOption.UseChargeAttack))
                {
                    //log.Info($"{Name}.MoveTo({target.Name})");

                    // charge attack
                    MoveTo(target);
                }
                else
                {
                    //log.Info($"{Name}.CreateMoveToChain({target.Name})");

                    CreateMoveToChain(target, (success) =>
                    {
                        if (success)
                            Attack(target, attackSequence);
                        else
                            OnAttackDone();
                    });
                }
            }
        }

        public void OnAttackDone(WeenieError error = WeenieError.None)
        {
            // this function is called at the very end of an attack sequence,
            // and not between the repeat attacks

            // it sends action cancelled so the power / accuracy meter
            // is reset, and doesn't start refilling again

            // the werror for this network message is not displayed to the client --
            // if you wish to display a message, a separate GameEventWeenieError should also be sent

            Session.Network.EnqueueSend(new GameEventAttackDone(Session, WeenieError.ActionCancelled));

            AttackTarget = null;
            MeleeTarget = null;
            MissileTarget = null;

            AttackQueue.Clear();

            AttackCancelled = false;
        }

        /// <summary>
        /// called when client sends the 'Cancel attack' network message
        /// </summary>
        public void HandleActionCancelAttack(WeenieError error = WeenieError.None)
        {
            //Console.WriteLine($"{Name}.HandleActionCancelAttack()");

            if (Attacking)
                AttackCancelled = true;
            else if (AttackTarget != null)
                OnAttackDone();

            PhysicsObj.cancel_moveto();
        }

        /// <summary>
        /// Performs a player melee attack against a target
        /// </summary>
        public void Attack(WorldObject target, int attackSequence, bool subsequent = false)
        {
            //log.Info($"{Name}.Attack({target.Name}, {attackSequence})");

            if (AttackSequence != attackSequence)
                return;

            if (CombatMode != CombatMode.Melee || MeleeTarget == null || IsBusy || !IsAlive || suicideInProgress)
            {
                OnAttackDone();
                return;
            }

            var creature = target as Creature;
            if (creature == null || !creature.IsAlive)
            {
                OnAttackDone();
                return;
            }

            var animLength = DoSwingMotion(target, out var attackFrames);
            if (animLength == 0)
            {
                OnAttackDone();
                return;
            }

            // point of no return beyond this point -- cannot be cancelled
            Attacking = true;

            if (subsequent)
            {
                // client shows hourglass, until attack done is received
                // retail only did this for subsequent attacks w/ repeat attacks on
                Session.Network.EnqueueSend(new GameEventCombatCommenceAttack(Session));
            }

            var weapon = GetEquippedMeleeWeapon();
            var attackType = GetWeaponAttackType(weapon);
            var numStrikes = GetNumStrikes(attackType);
            var swingTime = animLength / numStrikes / 1.5f;

            var actionChain = new ActionChain();

            // stamina usage
            // TODO: ensure enough stamina for attack
            var staminaCost = GetAttackStamina(GetPowerRange());
            UpdateVitalDelta(Stamina, -staminaCost);

            if (numStrikes != attackFrames.Count)
            {
                //log.Warn($"{Name}.GetAttackFrames(): MotionTableId: {MotionTableId:X8}, MotionStance: {CurrentMotionState.Stance}, Motion: {GetSwingAnimation()}, AttackFrames.Count({attackFrames.Count}) != NumStrikes({numStrikes})");
                numStrikes = attackFrames.Count;
            }

            // handle self-procs
            TryProcEquippedItems(this, this, true, weapon);

            var prevTime = 0.0f;
            int strikeCounter = 0;
            int successfulHitCounter = 0;

            for (var i = 0; i < numStrikes; i++)
            {
                // are there animation hooks for damage frames?
                //if (numStrikes > 1 && !TwoHandedCombat)
                //actionChain.AddDelaySeconds(swingTime);
                actionChain.AddDelaySeconds(attackFrames[i].time * animLength - prevTime);
                prevTime = attackFrames[i].time * animLength;

                actionChain.AddAction(this, ActionType.PlayerMelee_AttackInner, () =>
                {
                    strikeCounter++;
                    if (IsDead)
                    {
                        Attacking = false;
                        OnAttackDone();
                        return;
                    }

                    var damageEvent = DamageTarget(creature, weapon);

                    // Only increment successful hit counter when we actually deal damage
                    // This ensures procs are based on successful hits, not swing attempts
                    bool hitSuccessful = damageEvent != null && damageEvent.HasDamage;
                    if (hitSuccessful)
                        successfulHitCounter++;

                    // handle target procs on primary: default only first hit, or geometric decay if weapon allows multi-strike procs
                    float strikeMultiplier = 0f;
                    if (hitSuccessful)
                    {
                        if (weapon != null && weapon.AllowMultiStrikeProcs)
                        {
                            var r = weapon.MultiStrikeDecay;
                            if (r >= 1.0f)
                            {
                                // If r = 1.0f (stored 0.0f), no reduction
                                strikeMultiplier = 1.0f;
                            }
                            else
                            {
                                // Use successful hit counter for decay calculation
                                var exp = Math.Max(0, successfulHitCounter - 1);
                                strikeMultiplier = (float)Math.Pow(r, exp);
                            }
                        }
                        else
                        {
                            // only first successful hit rolls
                            strikeMultiplier = successfulHitCounter == 1 ? 1.0f : 0.0f;
                        }

                        if (strikeMultiplier > 0f)
                            TryProcEquippedItemsWithChanceMod(this, creature, false, weapon, strikeMultiplier);
                    }

                    if (weapon != null && weapon.IsCleaving && creature != null)
                    {
                        var cleave = GetCleaveTarget(creature, weapon);
                        
                        // Check if cleave list is null or empty
                        if (cleave == null || cleave.Count == 0)
                            return;
                        
                        // Apply cleave damage regardless of primary hit success
                        // Procs only happen if primary hit was successful (strikeMultiplier > 0f)
                        // strikeMultiplier is already calculated above and matches primary target behavior
                        foreach (var cleaveHit in cleave)
                        {
                            // Skip null cleave targets
                            if (cleaveHit == null)
                                continue;

                            // Apply cast on strike effects to cleaved targets
                            var cleaveDamageEvent = DamageTarget(cleaveHit, weapon);
                            
                            // Handle weapon-only procs for cleaved targets: uses strike-decayed chance then applies cleave decay
                            // Only proc if primary hit was successful and dealt damage
                            // strikeMultiplier will be 1.0f on first successful hit even if AllowMultiStrikeProcs is false
                            if (cleaveDamageEvent != null && cleaveDamageEvent.HasDamage && strikeMultiplier > 0f)
                            {
                                TryProcWeaponOnCleaveTarget(this, cleaveHit, weapon, weapon?.CleaveTargets ?? 1, successfulHitCounter, strikeMultiplier);
                            }
                        }
                    }
                });

                //if (numStrikes == 1 || TwoHandedCombat)
                    //actionChain.AddDelaySeconds(swingTime);
            }

            //actionChain.AddDelaySeconds(animLength - swingTime * numStrikes);
            actionChain.AddDelaySeconds(animLength - prevTime);

            actionChain.AddAction(this, ActionType.PlayerMelee_PowerbarRefill, () =>
            {
                Attacking = false;

                // powerbar refill timing
                var refillMod = IsDualWieldAttack ? 0.8f : 1.0f;    // dual wield powerbar refills 20% faster

                PowerLevel = AttackQueue.Fetch();

                var nextRefillTime = PowerLevel * refillMod;
                NextRefillTime = DateTime.UtcNow.AddSeconds(nextRefillTime);

                var dist = GetCylinderDistance(target);

                bool isFlickerStrike = weapon.GetProperty(PropertyBool.FlickerStrike) ?? false;
                if (isFlickerStrike)
                {
                    var allObjects = creature.CurrentLandblock.GetWorldObjectsForPhysicsHandling();
                    float currentDist = 10000f;
                    foreach (var wo in allObjects)
                    {
                        if (wo is Creature c && c.Attackable && c is not Player && c != creature && !c.IsDead)
                        {
                            float d = c.GetDistance(this);
                            if (d <= currentDist)
                            {
                                currentDist = d;
                                creature = c;
                            }
                        }
                    }
                    TurnToObject(creature, true, 10000f);
                }

                if (creature.IsAlive && GetCharacterOption(CharacterOption.AutoRepeatAttacks) && (dist <= (MeleeDistance + (LumAugmentMeleeRange ?? 0) * 0.1f) || dist <= StickyDistance && IsMeleeVisible(creature)) && !IsBusy && !AttackCancelled)
                {
                    // client starts refilling power meter
                    Session.Network.EnqueueSend(new GameEventAttackDone(Session));

                    var nextAttack = new ActionChain();
                    nextAttack.AddDelaySeconds(nextRefillTime);
                    if (isFlickerStrike)
                    {
                        nextAttack.AddAction(this, ActionType.PlayerMelee_Attack, () => LaunchFlickerProjectile(creature));
                    }
                    else
                    {
                        nextAttack.AddAction(this, ActionType.PlayerMelee_Attack, () => Attack(creature, attackSequence, true));
                    }
                    nextAttack.EnqueueChain();
                }
                else
                    OnAttackDone();
            });

            actionChain.EnqueueChain();

            if (UnderLifestoneProtection)
                LifestoneProtectionDispel();
        }

        /// <summary>
        /// Performs the player melee swing animation
        /// </summary>
        public float DoSwingMotion(WorldObject target, out List<(float time, AttackHook attackHook)> attackFrames)
        {
            // get the proper animation speed for this attack,
            // based on weapon speed and player quickness
            var baseSpeed = GetAnimSpeed();
            var animSpeedMod = IsDualWieldAttack ? 1.2f : 1.0f;     // dual wield swing animation 20% faster
            var animSpeed = baseSpeed * animSpeedMod;

            var swingAnimation = GetSwingAnimation();
            var animLength = MotionTable.GetAnimationLength(MotionTableId, CurrentMotionState.Stance, swingAnimation, animSpeed);
            //Console.WriteLine($"AnimSpeed: {animSpeed}, AnimLength: {animLength}");

            attackFrames = MotionTable.GetAttackFrames(MotionTableId, CurrentMotionState.Stance, swingAnimation);
            //Console.WriteLine($"Attack frames: {string.Join(",", attackFrames)}");

            // broadcast player swing animation to clients
            var motion = new Motion(this, swingAnimation, animSpeed);
            if (ServerConfig.persist_movement.Value)
            {
                motion.Persist(CurrentMotionState);
            }
            motion.MotionState.TurnSpeed = 2.25f;
            motion.MotionFlags |= MotionFlags.StickToObject;
            motion.TargetGuid = target.Guid;
            CurrentMotionState = motion;

            EnqueueBroadcastMotion(motion);

            if (FastTick)
                PhysicsObj.stick_to_object(target.Guid.Full);

            return animLength;
        }

        public static readonly float KickThreshold = 0.75f;

        public MotionCommand PrevMotionCommand;

        /// <summary>
        /// Returns the melee swing animation - based on weapon,
        /// current stance, power bar, and attack height
        /// </summary>
        public MotionCommand GetSwingAnimation()
        {
            if (IsDualWieldAttack)
                DualWieldAlternate = !DualWieldAlternate;

            var offhand = IsDualWieldAttack && !DualWieldAlternate;

            var weapon = GetEquippedMeleeWeapon();

            // for reference: https://www.youtube.com/watch?v=MUaD53D9c74
            // a player with 1/2 power bar, or slightly below half
            // doing the backswing, well above 33%
            var subdivision = 0.33f;

            if (weapon != null)
            {
                AttackType = weapon.GetAttackType(CurrentMotionState.Stance, PowerLevel, offhand);
                if (weapon.IsThrustSlash)
                    subdivision = 0.66f;
            }
            else
            {
                AttackType = PowerLevel > KickThreshold && !IsDualWieldAttack ? AttackType.Kick : AttackType.Punch;
            }

            var motions = CombatTable.GetMotion(CurrentMotionState.Stance, AttackHeight.Value, AttackType, PrevMotionCommand);

            // higher-powered animation always in first slot ?
            var motion = motions.Count > 1 && PowerLevel < subdivision ? motions[1] : motions[0];

            PrevMotionCommand = motion;

            //Console.WriteLine($"{motion}");

            return motion;
        }
        public void LaunchFlickerProjectile(Creature target)
        {
            // Spell ID 95: Force Bolt I
            var spell = new Spell((uint)SpellId.ForceBolt1);
            
            if (WorldObjectFactory.CreateNewWorldObject(spell.Wcid) is SpellProjectile sp)
            {
                // Setup
                sp.Setup(spell, ProjectileSpellType.Bolt, null); 
                
                // Target Direction
                // Ensure we use Global coordinates to handle different cells/landblocks correctly
                var playerPosGlobal = Location.ToGlobal();
                var targetPosGlobal = target.Location.ToGlobal();
                
                // Aim at center of mass
                var aimSource = playerPosGlobal + new System.Numerics.Vector3(0, 0, Height * 0.66f);
                var aimTarget = targetPosGlobal + new System.Numerics.Vector3(0, 0, target.Height * 0.66f);
                
                var dir = System.Numerics.Vector3.Normalize(aimTarget - aimSource);
                
                // 1. Turn to face target
                Location.Rotate(dir);
                PhysicsObj.Position.Frame.Orientation = Location.Rotation; // Force physics update too
                
                // 2. Calculate Spawn Origin (Front of player + buffer)
                // Use physics radius + projectile radius estimate (approx 0.1f) + safety buffer
                var radius = PhysicsObj.GetPhysicsRadius() + 0.1f + 0.2f;
                // Use LOCAL coordinates for the spawn position assignment
                var aimSourceLocal = Location.Pos + new System.Numerics.Vector3(0, 0, Height * 0.66f);
                var startVector = aimSourceLocal + (dir * radius);
                
                sp.Location = new Position(Location);
                sp.Location.Pos = startVector;
                sp.Location.Rotation = Location.Rotation;

                // Velocity: High speed direct shot
                var speed = 500f; // Very fast
                sp.PhysicsObj.Velocity = dir * speed;

                // Orientation
                // var dir = System.Numerics.Vector3.Normalize(sp.Velocity);
                sp.PhysicsObj.Position.Frame.set_vector_heading(dir);
                sp.Location.Rotation = sp.PhysicsObj.Position.Frame.Orientation;

                // Properties
                sp.ProjectileSource = this;
                sp.ProjectileTarget = target;
                sp.ProjectileLauncher = GetEquippedMeleeWeapon();
                sp.SpawnPos = new Position(sp.Location);
                
                // Set FlickerStrike property on projectile to trigger teleport logic on impact
                sp.SetProperty(PropertyBool.FlickerStrike, true);

                if (LandblockManager.AddObject(sp))
                {
                     sp.EnqueueBroadcast(new GameMessageScript(sp.Guid, PlayScript.Launch, sp.GetProjectileScriptIntensity(ProjectileSpellType.Bolt)));
                }
                else
                {
                    sp.Destroy();
                }
            }
        }

        public void Blink(Position destPos)
        {
            var newPosition = new Position(destPos);
            newPosition.PositionZ += 0.005f * (ObjScale ?? 1.0f);
            
            Console.WriteLine($"[Blink] Rotation Check: {newPosition.Rotation.X} {newPosition.Rotation.Y} {newPosition.Rotation.Z} {newPosition.Rotation.W}");

            UpdatePosition(newPosition, true);
            
            // Explicitly sync Physics Orientation ensuring the packet carries the new rotation
            if (PhysicsObj != null)
            {
                PhysicsObj.Position.Frame.Orientation = newPosition.Rotation;
                PhysicsObj.Omega = System.Numerics.Vector3.Zero; // Stop spinning
            }

            // Increment ForcePosition sequence to make client accept the move without a portal screen
            Sequences.GetNextSequence(Network.Sequence.SequenceType.ObjectForcePosition);
            Session.Network.EnqueueSend(new GameMessageUpdatePosition(this, false));
        }

        public void FinishFlickerStrike(Creature target)
        {
            if (target == null) return;

            // Turn very quickly to face the target.
            TurnToObject(target, true, 1000f);
            // Explicitly finish the attack sequence
            // Bypass Flicker checks, distance checks (assume we teleported close enough), etc.
            MeleeTarget = target;
            Attack(target, 0); // 0 = first attack in sequence? or pass sequence?
        }
    }
}
