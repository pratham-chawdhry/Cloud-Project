package com.worker.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApiResponse<T> {
    private String status;
    private int code;
    private String errorMessage;
    private T payload;

    public static <T> ApiResponse<T> success(int code, T payload) {
        return new ApiResponse<>("success", code, null, payload);
    }

    public static <T> ApiResponse<T> fail(int code, String message) {
        return new ApiResponse<>("fail", code, message, null);
    }
}
