import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class CassandraService {
    //protected static readonly ServiceUrl: string = 'http://172.18.0.1:5001/';

	protected static readonly ServiceUrl: string = 'http://localhost:5001/';
    constructor(private http: HttpClient) {

    }

    public async getKeyspaces() {
        const query = 'describe keyspaces;';
        console.log('keyspaces described!');
    }

    public async insert() {
        console.log('Insert!');

        return await this.httpPost<any, any>(`insert`, null);
    }

    public async get() {
        console.log('Get!');
        return await this.httpGet<any>(`predictions`, null);
    }

    protected async httpGet<TResponse>(
		endpoint: string,
		params?: any
	): Promise<TResponse> {
		const httpParams = this.getRequestParams(params);
		const composedUrl = `${this.serviceUrl}${endpoint}`;
		const response = await this.http
			.get<TResponse>(`${this.serviceUrl}${endpoint}`, {
			headers: this.getRequestHeaders(),
			params: httpParams,
			})
			.toPromise<TResponse>();

		return response;
	}

	protected async httpPost<TResponse, TBody>(
		endpoint: string,
		object: TBody,
		params?: any
	): Promise<TResponse> {
		const httpParams = this.getRequestParams(params);

		const response = await this.http
			.post<TResponse>(`${this.serviceUrl}${endpoint}`, object, {
                headers: this.getRequestHeaders(),
                params: httpParams,
			})
			.toPromise<TResponse>();

		return response;
	}

    protected get serviceUrl(): string {
		return CassandraService.ServiceUrl;
	}

    protected getRequestParams(params: any): any {
        if (params == null) { return null }

		let httpParams = new HttpParams();
		Object.keys(params).forEach((key) => {
			if (params[key] != null) {
			    httpParams = httpParams.append(key, params[key]);
			}
		});
		return httpParams;
	}

	protected getRequestHeaders(): HttpHeaders {
		const headers = new HttpHeaders({
			'Content-Type': 'application/json; charset=utf-8',
			'Cache-Control': 'no-cache, max-age=0',
			Pragma: 'no-cache',
		});
		return headers;
	}
    
}
