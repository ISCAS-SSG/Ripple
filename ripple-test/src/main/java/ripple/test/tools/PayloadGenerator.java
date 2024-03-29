// Copyright (c) 2023 Institute of Software, Chinese Academy of Sciences
// Ripple is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ripple.test.tools;

import java.util.Random;

public class PayloadGenerator {
    private PayloadGenerator() {

    }

    private static final String DICT = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

    public static String generateKey(int size) {
        StringBuilder builder = new StringBuilder();
        Random random = new Random();
        int i = 0;
        for (i = 0; i < size; i++) {
            builder.append(DICT.charAt(random.nextInt(DICT.length())));
        }
        return builder.toString();
    }

    public static String generateKeyValuePair(int count, int sizePerEntry) {
        StringBuilder builder = new StringBuilder();
        Random random = new Random();
        int i = 0;
        int j = 0;
        for (i = 0; i < count; i++) {
            int keySize = random.nextInt(sizePerEntry - 3) + 1;
            int valueSize = sizePerEntry - 1 - 1 - keySize;
            for (j = 0; j < keySize; j++) {
                builder.append(DICT.charAt(random.nextInt(DICT.length())));
            }
            builder.append("=");
            for (j = 0; j < valueSize; j++) {
                builder.append(DICT.charAt(random.nextInt(DICT.length())));
            }
            builder.append(";");
        }
        return builder.toString();
    }

    public static void main(String[] args) {
        String key = PayloadGenerator.generateKey(16);
        System.out.println("Key = " + key);
        String value = PayloadGenerator.generateKeyValuePair(5, 10);
        System.out.println("Value = " + value);
    }
}
