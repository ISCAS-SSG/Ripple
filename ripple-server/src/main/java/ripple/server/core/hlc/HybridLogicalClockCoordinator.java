// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.server.core.hlc;

import java.util.UUID;

/**
 * @author Zhen Tang
 */
public class HybridLogicalClockCoordinator {
    private static final long STEPS = 1;
    private HybridLogicalClock current;
    private long maxOffset;

    public HybridLogicalClockCoordinator(long maxOffset) {
        this.setCurrent(new HybridLogicalClock(UUID.randomUUID().toString()));
        this.setMaxOffset(maxOffset);
    }

    public HybridLogicalClock getCurrent() {
        return current;
    }

    private void setCurrent(HybridLogicalClock current) {
        this.current = current;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    private void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    private long getPhysicalTime() {
        return System.currentTimeMillis();
    }

    public synchronized HybridLogicalClock generateForSending() {
        long currentWallTime = this.getCurrent().getWallTime();
        long currentLogicalClock = this.getCurrent().getLogicalClock();
        long currentPhysicalTime = this.getPhysicalTime();
        long targetWallTime = Math.max(currentWallTime, currentPhysicalTime);
        long targetLogicalClock;
        if (targetWallTime == currentWallTime) {
            targetLogicalClock = currentLogicalClock + HybridLogicalClockCoordinator.STEPS;
        } else {
            targetLogicalClock = 0;
        }
        this.getCurrent().set(targetWallTime, targetLogicalClock);
        return new HybridLogicalClock(this.getCurrent().getProcessName(), targetWallTime, targetLogicalClock);
    }

    public synchronized HybridLogicalClock generateForReceiving(HybridLogicalClock remoteClock) {
        long currentWallTime = this.getCurrent().getWallTime();
        long currentLogicalClock = this.getCurrent().getLogicalClock();
        long remoteWallTime = remoteClock.getWallTime();
        long remoteLogicalClock = remoteClock.getLogicalClock();
        long currentPhysicalTime = this.getPhysicalTime();
        long targetWallTime = Math.max(currentWallTime, Math.max(remoteWallTime, currentPhysicalTime));
        long targetLogicalClock;
        if (targetWallTime == currentWallTime && targetWallTime == remoteWallTime) {
            targetLogicalClock = Math.max(currentLogicalClock, remoteLogicalClock) + HybridLogicalClockCoordinator.STEPS;
        } else if (targetWallTime == currentWallTime) {
            targetLogicalClock = currentLogicalClock + HybridLogicalClockCoordinator.STEPS;
        } else if (targetWallTime == remoteWallTime) {
            targetLogicalClock = remoteLogicalClock + HybridLogicalClockCoordinator.STEPS;
        } else {
            targetLogicalClock = 0;
        }
        this.getCurrent().set(targetWallTime, targetLogicalClock);
        return new HybridLogicalClock(this.getCurrent().getProcessName(), targetWallTime, targetLogicalClock);
    }

    public boolean isHappenBefore(HybridLogicalClock former, HybridLogicalClock latter) {
        // Currently, for events happen on different processes, it returns true only if the former event
        // happens before the latter event in real time.
        // The maximum offset of real time between two nodes is defined in the field "maxOffset".
        if (former.getProcessName().equals(latter.getProcessName())) {
            return former.smallerThan(latter);
        } else {
            return ((former.getWallTime() + this.getMaxOffset()) < latter.getWallTime());
        }
    }

    public boolean isConcurrent(HybridLogicalClock former, HybridLogicalClock latter) {
        return (!this.isHappenBefore(former, latter) && !this.isHappenBefore(latter, former));
    }
}
