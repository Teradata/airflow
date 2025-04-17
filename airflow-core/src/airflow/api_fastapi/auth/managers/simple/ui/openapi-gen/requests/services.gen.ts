// This file is auto-generated by @hey-api/openapi-ts
import type { CancelablePromise } from "./core/CancelablePromise";
import { OpenAPI } from "./core/OpenAPI";
import { request as __request } from "./core/request";
import type {
  CreateTokenAllAdminsResponse,
  CreateTokenData,
  CreateTokenResponse,
  CreateTokenCliData,
  CreateTokenCliResponse,
} from "./types.gen";

export class SimpleAuthManagerLoginService {
  /**
   * Create Token All Admins
   * Create a token with no credentials only if ``simple_auth_manager_all_admins`` is True.
   * @returns LoginResponse Successful Response
   * @throws ApiError
   */
  public static createTokenAllAdmins(): CancelablePromise<CreateTokenAllAdminsResponse> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/auth/token",
      errors: {
        403: "Forbidden",
      },
    });
  }

  /**
   * Create Token
   * Authenticate the user.
   * @param data The data for the request.
   * @param data.requestBody
   * @returns LoginResponse Successful Response
   * @throws ApiError
   */
  public static createToken(data: CreateTokenData): CancelablePromise<CreateTokenResponse> {
    return __request(OpenAPI, {
      method: "POST",
      url: "/auth/token",
      body: data.requestBody,
      mediaType: "application/json",
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        422: "Validation Error",
      },
    });
  }

  /**
   * Login All Admins
   * Login the user with no credentials.
   * @throws ApiError
   */
  public static loginAllAdmins(): CancelablePromise<void> {
    return __request(OpenAPI, {
      method: "GET",
      url: "/auth/token/login",
      errors: {
        307: "Successful Response",
        403: "Forbidden",
      },
    });
  }

  /**
   * Create Token Cli
   * Authenticate the user for the CLI.
   * @param data The data for the request.
   * @param data.requestBody
   * @returns LoginResponse Successful Response
   * @throws ApiError
   */
  public static createTokenCli(data: CreateTokenCliData): CancelablePromise<CreateTokenCliResponse> {
    return __request(OpenAPI, {
      method: "POST",
      url: "/auth/token/cli",
      body: data.requestBody,
      mediaType: "application/json",
      errors: {
        400: "Bad Request",
        401: "Unauthorized",
        422: "Validation Error",
      },
    });
  }
}
