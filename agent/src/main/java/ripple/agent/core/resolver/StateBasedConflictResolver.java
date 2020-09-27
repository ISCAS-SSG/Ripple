/*
 * Copyright 1999-2019 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ripple.agent.core.resolver;

import ripple.agent.core.hlc.Datum;

/**
 * Public interface for a state based conflict resolver.
 *
 * @author lostcharlie
 */
public interface StateBasedConflictResolver {
    /**
     * Apply target state on local data replica and resolve conflicts for concurrent updates
     *
     * @param current current value of the datum
     * @param target  target value of the datum
     */
    void merge(Datum current, Datum target);
}
