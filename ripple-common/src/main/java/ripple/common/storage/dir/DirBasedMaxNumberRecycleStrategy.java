// Copyright (c) 2024 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.common.storage.dir;

import ripple.common.storage.RecycleStrategy;

/**
 * @author Zhen Tang
 */
public class DirBasedMaxNumberRecycleStrategy implements RecycleStrategy {
    private DirBasedStorage storage;
    private int maxNumberOfMessages;

    public DirBasedStorage getStorage() {
        return storage;
    }

    public void setStorage(DirBasedStorage storage) {
        this.storage = storage;
    }

    public int getMaxNumberOfMessages() {
        return maxNumberOfMessages;
    }

    public void setMaxNumberOfMessages(int maxNumberOfMessages) {
        this.maxNumberOfMessages = maxNumberOfMessages;
    }

    public DirBasedMaxNumberRecycleStrategy(DirBasedStorage storage, int maxNumberOfMessages) {
        this.setStorage(storage);
        this.setMaxNumberOfMessages(maxNumberOfMessages);
    }

    @Override
    public synchronized void recycle(String applicationName, String key) {

    }
}