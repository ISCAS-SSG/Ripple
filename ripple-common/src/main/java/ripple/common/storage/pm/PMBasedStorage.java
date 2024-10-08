// Copyright (c) 2024 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.common.storage.pm;

import ripple.common.storage.AckService;
import ripple.common.storage.ItemService;
import ripple.common.storage.MessageService;
import ripple.common.storage.RecycleStrategy;
import ripple.common.storage.Storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author Zhen Tang
 */
public class PMBasedStorage implements Storage {
    private String location;
    private String cacheLocation;
    private ItemService itemService;
    private MessageService messageService;
    private AckService ackService;
    private RecycleStrategy recycleStrategy;
    private PMCacheAdapter pmCacheAdapter;

    @Override
    public String getLocation() {
        return location;
    }

    private void setLocation(String location) {
        this.location = location;
    }

    public String getCacheLocation() {
        return cacheLocation;
    }

    public void setCacheLocation(String cacheLocation) {
        this.cacheLocation = cacheLocation;
    }

    @Override
    public ItemService getItemService() {
        return itemService;
    }

    private void setItemService(ItemService itemService) {
        this.itemService = itemService;
    }

    @Override
    public MessageService getMessageService() {
        return messageService;
    }

    private void setMessageService(MessageService messageService) {
        this.messageService = messageService;
    }

    @Override
    public AckService getAckService() {
        return ackService;
    }

    private void setAckService(AckService ackService) {
        this.ackService = ackService;
    }

    @Override
    public RecycleStrategy getRecycleStrategy() {
        return recycleStrategy;
    }

    public void setRecycleStrategy(RecycleStrategy recycleStrategy) {
        this.recycleStrategy = recycleStrategy;
    }

    public PMCacheAdapter getPmCacheAdapter() {
        return pmCacheAdapter;
    }

    public void setPmCacheAdapter(PMCacheAdapter pmCacheAdapter) {
        this.pmCacheAdapter = pmCacheAdapter;
    }

    public PMBasedStorage(String location, String cacheLocation, int maxNumberOfMessagePerItem) {
        this.setLocation(location);
        this.setCacheLocation(cacheLocation);
        this.setItemService(new PMBasedItemService(this));
        this.setMessageService(new PMBasedMessageService(this));
        this.setAckService(new PMBasedAckService(this));
        this.setRecycleStrategy(new PMBasedMaxNumberRecycleStrategy(this, maxNumberOfMessagePerItem));
        this.setPmCacheAdapter(new PMCacheAdapter());
        this.init();
    }

    private void init() {
        try {
            Files.createDirectories(Paths.get(this.getLocation()));
            this.getPmCacheAdapter().open(this.getCacheLocation(), this.getLocation());
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    @Override
    public void close() {
        this.getPmCacheAdapter().close();
    }

}
