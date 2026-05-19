import { Component, OnInit } from '@angular/core';
import { FaresService } from './services/fares.service';
import { HttpClient } from '@angular/common/http';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  title = 'Fares API Gateway UI';
  flightId = '';
  origin = '';
  destination = '';
  originsList: string[] = [];
  destinationsList: string[] = [];
  fareResult: any = null;
  searchResult: any = null;
  tableColumns: string[] = ['flight_id', 'origin', 'destination', 'fare', 'currency', 'available_seats'];
  tableData: any[] = [];
  errorMessage = '';
  loading = false;

  constructor(private faresService: FaresService, private http: HttpClient) {}

  ngOnInit(): void {
    this.loadFaresCsv();
  }

  private loadFaresCsv(): void {
    this.http.get('assets/fares.csv', { responseType: 'text' }).subscribe({
      next: (csv) => {
        const origins = new Set<string>();
        const destinations = new Set<string>();
        const lines = csv.split(/\r?\n/);
        // skip header
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i].trim();
          if (!line) continue;
          const parts = line.split(',');
          // CSV layout: flight_id,origin,destination,...
          const origin = (parts[1] || '').trim();
          const dest = (parts[2] || '').trim();
          if (origin) origins.add(origin);
          if (dest) destinations.add(dest);
        }
        this.originsList = Array.from(origins).sort();
        this.destinationsList = Array.from(destinations).sort();
      },
      error: () => {
        // non-fatal: leave lists empty
      }
    });
  }

  search(): void {
    this.clearMessages();
    if (this.flightId.trim()) {
      this.loadFareById();
      return;
    }

    this.loadSearchResults();
  }

  private loadFareById(): void {
    this.loading = true;
    this.faresService.getFare(this.flightId).subscribe({
      next: (result) => {
        this.fareResult = result;
        this.searchResult = null;
        this.tableData = [result.consensus_fare || result];
        this.loading = false;
      },
      error: (error) => {
        this.errorMessage = error?.error?.detail || error?.message || 'Unknown error connecting to fares API.';
        this.loading = false;
      }
    });
  }

  private loadSearchResults(): void {
    this.loading = true;
    this.faresService.searchFares(this.origin, this.destination).subscribe({
      next: (result) => {
        this.searchResult = result;
        this.fareResult = null;
        this.tableData = result?.fares || result?.consensus_results || [];
        this.loading = false;
      },
      error: (error) => {
        this.errorMessage = error?.error?.detail || error?.message || 'Unknown error connecting to fares API.';
        this.loading = false;
      }
    });
  }

  private clearMessages(): void {
    this.errorMessage = '';
    this.fareResult = null;
    this.searchResult = null;
    this.tableData = [];
  }
}
