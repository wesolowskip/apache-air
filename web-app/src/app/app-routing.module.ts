import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { CitiesComponent } from './components/cities/cities.component';
import { MainComponent } from './components/main/main.component';
import { MapComponent } from './components/map/map.component';

const routes: Routes = [
  { path: 'main', component: MainComponent },
  { path: 'cities', component: CitiesComponent },
  { path: 'map', component: MapComponent },
  { path: '', redirectTo: 'map' ,  pathMatch: 'full' }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
