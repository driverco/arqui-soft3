import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

@Injectable({
  providedIn: 'root'
})
export class FaresService {
  private baseUrl = environment.apiUrl;

  constructor(private http: HttpClient) {}

  getFare(flightId: string): Observable<any> {
    return this.http.get<any>(`${this.baseUrl}/faresapi/get-fares/${encodeURIComponent(flightId)}`);
  }

  searchFares(origin?: string, destination?: string): Observable<any> {
    let params = new HttpParams();
    if (origin) {
      params = params.set('origin', origin);
    }
    if (destination) {
      params = params.set('destination', destination);
    }
    return this.http.get<any>(`${this.baseUrl}/faresapi/search-fares`, { params });
  }
}
