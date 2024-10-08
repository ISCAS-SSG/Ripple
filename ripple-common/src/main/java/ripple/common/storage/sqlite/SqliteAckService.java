// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.common.storage.sqlite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import ripple.common.entity.Ack;
import ripple.common.storage.AckService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zhen Tang
 */
public class SqliteAckService implements AckService {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private SqliteStorage storage;
    private ConcurrentHashMap<UUID, Object> locks;

    public SqliteStorage getStorage() {
        return storage;
    }

    private void setStorage(SqliteStorage storage) {
        this.storage = storage;
    }

    private ConcurrentHashMap<UUID, Object> getLocks() {
        return locks;
    }

    private void setLocks(ConcurrentHashMap<UUID, Object> locks) {
        this.locks = locks;
    }

    public SqliteAckService(SqliteStorage storage) {
        this.setStorage(storage);
        this.setLocks(new ConcurrentHashMap<>());
    }

    private synchronized Object getLock(UUID messageUuid) {
        if (!this.getLocks().containsKey(messageUuid)) {
            this.getLocks().put(messageUuid, new Object());
        }
        return this.getLocks().get(messageUuid);
    }

    @Override
    public boolean initAck(UUID messageUuid, List<Integer> nodeList) {
        synchronized (this.getLock(messageUuid)) {
            try {
                String nodeListString = MAPPER.writeValueAsString(new HashSet<>(nodeList));
                String ackNodesString = MAPPER.writeValueAsString(new HashSet<>());
                Connection connection = this.getStorage().getConnection();
                String sql = "INSERT INTO [ack] ([message_uuid], [node_list], [ack_nodes]) VALUES (?,?,?);";
                PreparedStatement statement = connection.prepareStatement(sql);
                statement.setString(1, messageUuid.toString());
                statement.setString(2, nodeListString);
                statement.setString(3, ackNodesString);
                int count = statement.executeUpdate();
                return count == 1;
            } catch (Exception exception) {
                exception.printStackTrace();
                return false;
            }
        }
    }

    @Override
    public Ack getAck(UUID messageUuid) {
        try {
            Connection connection = this.getStorage().getConnection();
            String sql = "SELECT * FROM [ack] WHERE [message_uuid] = ?;";
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setString(1, messageUuid.toString());
            ResultSet resultSet = statement.executeQuery();

            Ack ack = null;

            if (resultSet.next()) {
                ack = parseAck(resultSet);
            }
            resultSet.close();
            return ack;
        } catch (Exception exception) {
            exception.printStackTrace();
            return null;
        }
    }

    private Ack parseAck(ResultSet resultSet) throws SQLException, JsonProcessingException {
        JavaType listType = MAPPER.getTypeFactory().constructCollectionType(HashSet.class, Integer.class);
        Ack ack = new Ack();
        ack.setMessageUuid(UUID.fromString(resultSet.getString("message_uuid")));
        ack.setNodeList(MAPPER.readValue(resultSet.getString("node_list"), listType));
        ack.setAckNodes(MAPPER.readValue(resultSet.getString("ack_nodes"), listType));
        return ack;
    }

    @Override
    public List<Ack> getAllAcks() {
        try {
            Connection connection = this.getStorage().getConnection();
            String sql = "SELECT * FROM [ack];";
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            List<Ack> ret = new ArrayList<>();
            while (resultSet.next()) {
                ret.add(this.parseAck(resultSet));
            }
            resultSet.close();
            return ret;
        } catch (Exception exception) {
            exception.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean recordAck(UUID messageUuid, int serverId) {
        synchronized (this.getLock(messageUuid)) {
            try {
                Ack ack = this.getAck(messageUuid);
                if (!ack.getAckNodes().contains(serverId)) {
                    ack.getAckNodes().add(serverId);
                }
                String newAckNodes = MAPPER.writeValueAsString(ack.getAckNodes());
                Connection connection = this.getStorage().getConnection();
                String sql = "UPDATE [ack] SET [ack_nodes] = ? WHERE [message_uuid] = ?;";
                PreparedStatement statement = connection.prepareStatement(sql);
                statement.setString(1, newAckNodes);
                statement.setString(2, messageUuid.toString());
                int count = statement.executeUpdate();
                return count == 1;
            } catch (Exception exception) {
                exception.printStackTrace();
                return false;
            }
        }
    }

    @Override
    public boolean removeAck(UUID messageUuid) {
        synchronized (this.getLock(messageUuid)) {
            try {
                Ack ack = this.getAck(messageUuid);
                if (ack == null) {
                    return false;
                }
                Connection connection = this.getStorage().getConnection();
                String sql = "DELETE FROM [ack] WHERE [message_uuid] = ?;";
                PreparedStatement statement = connection.prepareStatement(sql);
                statement.setString(1, messageUuid.toString());
                int count = statement.executeUpdate();
                return count == 1;
            } catch (Exception exception) {
                exception.printStackTrace();
                return false;
            }
        }
    }
}
